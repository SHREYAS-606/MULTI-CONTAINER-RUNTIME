/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;        /* set before sending SIGTERM from stop command */
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* Global supervisor context pointer used by signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

/* ---------------------------------------------------------------
 * Helpers: usage, flag parsing, state string
 * --------------------------------------------------------------- */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

/* ---------------------------------------------------------------
 * Bounded Buffer Implementation
 * --------------------------------------------------------------- */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * bounded_buffer_push - Producer inserts a log chunk into the ring buffer.
 *
 * Blocks when the buffer is full until space is available or shutdown begins.
 * Returns 0 on success, -1 if the buffer is shutting down.
 *
 * Why mutex + condition variable:
 *   Multiple producer threads (one per container pipe reader) and one consumer
 *   thread access the shared ring buffer concurrently. Without the mutex,
 *   two producers could both read count < CAPACITY and both write to the same
 *   slot, corrupting data. The condition variable lets blocked producers sleep
 *   efficiently instead of spinning, and the consumer wakes them only when
 *   space opens up.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while full and not shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    /* If shutdown started, reject new items */
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* Wake the consumer */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop - Consumer removes a log chunk from the ring buffer.
 *
 * Blocks when the buffer is empty until data arrives or shutdown begins.
 * Returns 0 on success, -1 when shutdown is in progress and buffer is empty
 * (signals the consumer thread to exit).
 *
 * Race condition prevented:
 *   Without the mutex a consumer could read head while a producer is
 *   simultaneously writing tail and count, leading to reading uninitialised
 *   or partially-written data.  The condition variable prevents spurious
 *   wakeups from causing the consumer to run when there is truly nothing to do.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while empty and not shutting down */
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* Shutdown started and nothing left to drain → tell consumer to exit */
    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* Wake a producer that may be waiting for space */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ---------------------------------------------------------------
 * Logging Consumer Thread
 * --------------------------------------------------------------- */

/*
 * logging_thread - Drains the bounded buffer and writes to per-container logs.
 *
 * One consumer thread is started by the supervisor.  It pops log_item_t
 * structs from the shared bounded buffer and appends the data to the
 * appropriate per-container log file (identified by container_id).
 *
 * The thread exits when bounded_buffer_pop returns -1, which happens only
 * after bounded_buffer_begin_shutdown is called AND the buffer is fully
 * drained — guaranteeing no log lines are lost even if a container exits
 * abruptly just before shutdown.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open");
            continue;
        }

        /* Write the full chunk; loop to handle partial writes */
        size_t written = 0;
        while (written < item.length) {
            ssize_t n = write(fd, item.data + written, item.length - written);
            if (n < 0) {
                perror("logging_thread: write");
                break;
            }
            written += (size_t)n;
        }
        close(fd);
    }

    fprintf(stderr, "[supervisor] logging_thread: exiting, buffer drained\n");
    return NULL;
}

/* ---------------------------------------------------------------
 * Producer Thread: reads a container pipe and feeds the buffer
 * --------------------------------------------------------------- */

typedef struct {
    int read_fd;                        /* read end of the container's pipe */
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} pipe_reader_args_t;

/*
 * pipe_reader_thread - Reads container stdout/stderr and pushes to buffer.
 *
 * One of these threads is spawned per container.  It reads from the pipe
 * whose write end was dup2'd over the container's stdout/stderr inside
 * child_fn.  When the container exits its write end of the pipe is closed,
 * causing read() here to return 0, at which point the thread exits cleanly.
 */
static void *pipe_reader_thread(void *arg)
{
    pipe_reader_args_t *pra = (pipe_reader_args_t *)arg;
    log_item_t item;
    ssize_t n;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, pra->container_id, CONTAINER_ID_LEN - 1);

    while ((n = read(pra->read_fd, item.data, LOG_CHUNK_SIZE - 1)) > 0) {
        item.length = (size_t)n;
        /* Push returns -1 only during shutdown; stop feeding if so */
        if (bounded_buffer_push(pra->buffer, &item) != 0)
            break;
        memset(item.data, 0, sizeof(item.data));
    }

    close(pra->read_fd);
    free(pra);
    return NULL;
}

/* ---------------------------------------------------------------
 * Container Child Entry Point
 * --------------------------------------------------------------- */

/*
 * child_fn - Runs inside the new namespace after clone().
 *
 * Steps:
 *   1. Set container hostname to its ID (UTS namespace isolation).
 *   2. Mount /proc inside the rootfs so tools like ps work.
 *   3. chroot into the container's private rootfs copy.
 *   4. Redirect stdout and stderr to the pipe write fd so all output
 *      flows back to the supervisor logging pipeline.
 *   5. Apply the requested nice value for scheduling experiments.
 *   6. exec the requested command.
 *
 * Why chroot instead of pivot_root:
 *   chroot is simpler to implement and sufficient for this project's
 *   isolation requirements.  pivot_root is more thorough (it fully
 *   replaces the root mount and prevents ".." escape) but requires the
 *   rootfs to be a separate mount point, which adds setup complexity.
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    char proc_path[PATH_MAX];

    /* --- UTS isolation: give the container its own hostname --- */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
        perror("child_fn: sethostname");
        return 1;
    }

    /* --- Mount /proc before chroot so the path is correct --- */
    snprintf(proc_path, sizeof(proc_path), "%s/proc", cfg->rootfs);
    /* Create the mount point if it doesn't exist */
    mkdir(proc_path, 0555);
    if (mount("proc", proc_path, "proc", 0, NULL) != 0) {
        /* Non-fatal: /proc may already be mounted or rootfs may lack the dir */
        perror("child_fn: mount /proc (continuing)");
    }

    /* --- Filesystem isolation: chroot into the container's rootfs --- */
    if (chroot(cfg->rootfs) != 0) {
        perror("child_fn: chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("child_fn: chdir /");
        return 1;
    }

    /* --- Redirect stdout and stderr to the supervisor logging pipe --- */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("child_fn: dup2");
        return 1;
    }
    /* Close the original write fd now that it has been duplicated */
    if (cfg->log_write_fd != STDOUT_FILENO && cfg->log_write_fd != STDERR_FILENO)
        close(cfg->log_write_fd);

    /* --- Scheduling: apply nice value if set --- */
    if (cfg->nice_value != 0) {
        errno = 0;
        nice(cfg->nice_value);
        if (errno != 0)
            perror("child_fn: nice (continuing)");
    }

    /* --- Execute the requested command --- */
    char *const args[] = { cfg->command, NULL };
    execv(cfg->command, args);

    /* execv only returns on error */
    perror("child_fn: execv");
    return 1;
}

/* ---------------------------------------------------------------
 * Monitor registration helpers (provided, unchanged)
 * --------------------------------------------------------------- */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ---------------------------------------------------------------
 * Metadata helpers (run under ctx->metadata_lock)
 * --------------------------------------------------------------- */

static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if (strncmp(c->id, id, CONTAINER_ID_LEN) == 0)
            return c;
        c = c->next;
    }
    return NULL;
}

static void add_container(supervisor_ctx_t *ctx, container_record_t *rec)
{
    rec->next = ctx->containers;
    ctx->containers = rec;
}

/* ---------------------------------------------------------------
 * SIGCHLD handler: reap exited children, update metadata
 * --------------------------------------------------------------- */

/*
 * sigchld_handler - Reaps all available children without blocking.
 *
 * Called asynchronously when a child exits.  Uses WNOHANG so it does not
 * stall the supervisor event loop.  Loops until waitpid returns 0 (no more
 * children ready) because multiple children may exit between two deliveries
 * of SIGCHLD.
 *
 * Why this matters: if we do not call waitpid here, exited children become
 * zombies — they stay in the process table consuming a PID slot until the
 * parent explicitly reaps them.
 */
static void sigchld_handler(int sig)
{
    (void)sig;
    int status;
    pid_t pid;

    if (!g_ctx)
        return;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_ctx->metadata_lock);

        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->exit_code = WEXITSTATUS(status);
                    c->exit_signal = 0;
                    c->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    c->exit_signal = WTERMSIG(status);
                    c->exit_code = 128 + c->exit_signal;
                    /*
                     * Distinguish manual stop vs kernel hard-limit kill.
                     * stop_requested is set by the STOP command handler
                     * before SIGTERM is sent.
                     */
                    if (c->stop_requested)
                        c->state = CONTAINER_STOPPED;
                    else
                        c->state = CONTAINER_KILLED;
                }

                /* Unregister from kernel monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd, c->id, pid);

                break;
            }
            c = c->next;
        }

        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

/* ---------------------------------------------------------------
 * SIGINT / SIGTERM handler: orderly supervisor shutdown
 * --------------------------------------------------------------- */

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* ---------------------------------------------------------------
 * Supervisor: launch one container
 *
 * Called from the supervisor event loop when a START or RUN request
 * arrives.  Creates the pipe, calls clone() with the required namespace
 * flags, records metadata, starts a pipe-reader thread, and registers
 * the container with the kernel monitor.
 * --------------------------------------------------------------- */
static pid_t launch_container(supervisor_ctx_t *ctx,
                              const control_request_t *req)
{
    int pipefd[2];
    pid_t child_pid;
    char *child_stack;
    child_config_t *cfg;

    /* Create the logging pipe */
    if (pipe(pipefd) != 0) {
        perror("launch_container: pipe");
        return -1;
    }

    /* Build the child configuration (allocated on heap so it survives clone) */
    cfg = malloc(sizeof(*cfg));
    if (!cfg) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,       PATH_MAX - 1);
    strncpy(cfg->command, req->command,      CHILD_COMMAND_LEN - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipefd[1];   /* child writes here */

    /* Allocate clone stack (grows downward, so pass top) */
    child_stack = malloc(STACK_SIZE);
    if (!child_stack) {
        free(cfg);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    /*
     * clone() with namespace flags:
     *   CLONE_NEWPID  — container gets its own PID namespace (it is PID 1)
     *   CLONE_NEWUTS  — container gets its own hostname
     *   CLONE_NEWNS   — container gets its own mount namespace (so mounting
     *                   /proc inside doesn't affect the host)
     *   SIGCHLD       — deliver SIGCHLD to parent when child exits so our
     *                   SIGCHLD handler can reap it
     */
    child_pid = clone(child_fn,
                      child_stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      cfg);

    /* Parent closes the write end of the pipe */
    close(pipefd[1]);

    if (child_pid < 0) {
        perror("launch_container: clone");
        free(cfg);
        free(child_stack);
        close(pipefd[0]);
        return -1;
    }

    /* Stack is referenced by the child until exec; free after clone returns */
    free(child_stack);
    /* cfg is used by child_fn before exec; child exits so it's effectively freed */
    free(cfg);

    /* --- Record metadata --- */
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) {
        close(pipefd[0]);
        return child_pid;   /* still launched, just no metadata */
    }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid         = child_pid;
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->stop_requested   = 0;
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    add_container(ctx, rec);
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* --- Start pipe-reader producer thread --- */
    pipe_reader_args_t *pra = malloc(sizeof(*pra));
    if (pra) {
        pra->read_fd = pipefd[0];
        pra->buffer  = &ctx->log_buffer;
        strncpy(pra->container_id, req->container_id, CONTAINER_ID_LEN - 1);

        pthread_t tid;
        if (pthread_create(&tid, NULL, pipe_reader_thread, pra) != 0) {
            perror("launch_container: pthread_create pipe reader");
            close(pipefd[0]);
            free(pra);
        } else {
            pthread_detach(tid);   /* reader cleans itself up on exit */
        }
    } else {
        close(pipefd[0]);
    }

    /* --- Register with kernel memory monitor --- */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd,
                                  req->container_id,
                                  child_pid,
                                  req->soft_limit_bytes,
                                  req->hard_limit_bytes) != 0) {
            perror("launch_container: register_with_monitor (continuing)");
        }
    }

    fprintf(stderr, "[supervisor] launched container id=%s pid=%d\n",
            req->container_id, child_pid);
    return child_pid;
}

/* ---------------------------------------------------------------
 * Supervisor event loop helpers
 * --------------------------------------------------------------- */

/*
 * handle_ps - Format all container metadata into the response message.
 */
static void handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    char line[256];
    size_t off = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = ctx->containers;

    if (!c) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "(no containers)\n");
        resp->status = 0;
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    resp->message[0] = '\0';
    while (c && off < (size_t)(CONTROL_MESSAGE_LEN - 1)) {
        int n = snprintf(line, sizeof(line),
                         "%-16s pid=%-6d state=%-10s soft=%luMiB hard=%luMiB log=%s\n",
                         c->id,
                         c->host_pid,
                         state_to_string(c->state),
                         c->soft_limit_bytes >> 20,
                         c->hard_limit_bytes >> 20,
                         c->log_path);
        if (n <= 0)
            break;
        size_t copy = (size_t)n;
        if (off + copy >= (size_t)CONTROL_MESSAGE_LEN)
            copy = (size_t)CONTROL_MESSAGE_LEN - off - 1;
        memcpy(resp->message + off, line, copy);
        off += copy;
        c = c->next;
    }
    resp->message[off] = '\0';
    resp->status = 0;
    pthread_mutex_unlock(&ctx->metadata_lock);
}

/*
 * handle_logs - Read the container log file and send content in the response.
 */
static void handle_logs(supervisor_ctx_t *ctx,
                        const control_request_t *req,
                        control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = find_container(ctx, req->container_id);
    char log_path[PATH_MAX];
    if (c)
        strncpy(log_path, c->log_path, PATH_MAX - 1);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!c) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "ERROR: container '%s' not found\n", req->container_id);
        resp->status = -1;
        return;
    }

    int fd = open(log_path, O_RDONLY);
    if (fd < 0) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "ERROR: cannot open log '%s': %s\n", log_path, strerror(errno));
        resp->status = -1;
        return;
    }

    ssize_t n = read(fd, resp->message, CONTROL_MESSAGE_LEN - 1);
    close(fd);
    if (n < 0) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "ERROR: read failed\n");
        resp->status = -1;
    } else {
        resp->message[n] = '\0';
        resp->status = 0;
    }
}

/*
 * handle_stop - Send SIGTERM to a running container.
 *
 * Sets stop_requested BEFORE sending the signal so the SIGCHLD handler
 * classifies the exit as CONTAINER_STOPPED rather than CONTAINER_KILLED.
 */
static void handle_stop(supervisor_ctx_t *ctx,
                        const control_request_t *req,
                        control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = find_container(ctx, req->container_id);

    if (!c) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "ERROR: container '%s' not found\n", req->container_id);
        resp->status = -1;
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    if (c->state != CONTAINER_RUNNING && c->state != CONTAINER_STARTING) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "container '%s' is not running (state=%s)\n",
                 req->container_id, state_to_string(c->state));
        resp->status = 0;
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    /* Mark stop_requested before sending signal */
    c->stop_requested = 1;
    pid_t pid = c->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(pid, SIGTERM) != 0) {
        perror("handle_stop: kill");
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "ERROR: could not signal container '%s'\n", req->container_id);
        resp->status = -1;
        return;
    }

    snprintf(resp->message, CONTROL_MESSAGE_LEN,
             "sent SIGTERM to container '%s' (pid=%d)\n", req->container_id, pid);
    resp->status = 0;
}

/* ---------------------------------------------------------------
 * Supervisor: main event loop
 * --------------------------------------------------------------- */

/*
 * run_supervisor - Long-running supervisor process.
 *
 * Responsibilities:
 *   1. Open /dev/container_monitor for kernel integration.
 *   2. Create the UNIX domain socket control channel.
 *   3. Install signal handlers (SIGCHLD, SIGINT, SIGTERM).
 *   4. Start the logging consumer thread.
 *   5. Accept CLI connections in a loop, dispatch requests, send responses.
 *   6. On shutdown: stop containers, join logger, clean up.
 *
 * IPC design:
 *   Path A (logging)  — pipes from each container's stdout/stderr into the
 *                       supervisor's bounded buffer, drained by logging_thread.
 *   Path B (control)  — UNIX domain socket; each CLI invocation connects,
 *                       sends a control_request_t, receives a control_response_t,
 *                       and disconnects.  Using a different mechanism from the
 *                       logging pipes keeps the two concerns separate and
 *                       allows bidirectional structured replies.
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    /* Store global pointer for signal handlers */
    g_ctx = &ctx;

    /* --- Initialise metadata lock --- */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    /* --- Initialise bounded buffer --- */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* --- Create log directory --- */
    mkdir(LOG_DIR, 0755);

    /* --- 1) Open kernel monitor device --- */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[supervisor] WARNING: cannot open /dev/container_monitor: %s\n"
                        "             Memory monitoring will be disabled.\n",
                strerror(errno));
        /* Non-fatal: supervisor works without the kernel module */
    }

    /* --- 2) Create UNIX domain socket (control channel, Path B) --- */
    struct sockaddr_un addr;
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    unlink(CONTROL_PATH);   /* Remove stale socket from previous run */
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }
    if (listen(ctx.server_fd, 16) < 0) {
        perror("listen");
        goto cleanup;
    }
    fprintf(stderr, "[supervisor] control socket: %s\n", CONTROL_PATH);

    /* --- 3) Signal handlers --- */
    struct sigaction sa_chld, sa_term;

    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = sigchld_handler;
    sigemptyset(&sa_chld.sa_mask);
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = sigterm_handler;
    sigemptyset(&sa_term.sa_mask);
    sa_term.sa_flags = 0;
    sigaction(SIGINT,  &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    /* --- 4) Start logging consumer thread --- */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }
    fprintf(stderr, "[supervisor] logging thread started\n");

    fprintf(stderr, "[supervisor] ready. base-rootfs=%s\n", rootfs);

    /* --- 5) Event loop: accept and dispatch CLI connections --- */
    while (!ctx.should_stop) {
        int client_fd;
        struct sockaddr_un client_addr;
        socklen_t client_len = sizeof(client_addr);

        client_fd = accept(ctx.server_fd,
                           (struct sockaddr *)&client_addr,
                           &client_len);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;   /* interrupted by signal, check should_stop */
            perror("accept");
            break;
        }

        /* Read the request */
        control_request_t req;
        control_response_t resp;
        memset(&req,  0, sizeof(req));
        memset(&resp, 0, sizeof(resp));

        ssize_t n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
        if (n != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

        /* Dispatch */
        switch (req.kind) {

        case CMD_START: {
            pid_t pid = launch_container(&ctx, &req);
            if (pid < 0) {
                resp.status = -1;
                snprintf(resp.message, CONTROL_MESSAGE_LEN,
                         "ERROR: failed to launch container '%s'\n",
                         req.container_id);
            } else {
                resp.status = 0;
                snprintf(resp.message, CONTROL_MESSAGE_LEN,
                         "started container '%s' pid=%d\n",
                         req.container_id, pid);
            }
            break;
        }

        case CMD_RUN: {
            /*
             * RUN: launch the container and wait for it to finish.
             * We block in waitpid here which means we cannot serve other
             * CLI requests while this container runs — acceptable for the
             * project scope.
             */
            pid_t pid = launch_container(&ctx, &req);
            if (pid < 0) {
                resp.status = -1;
                snprintf(resp.message, CONTROL_MESSAGE_LEN,
                         "ERROR: failed to launch container '%s'\n",
                         req.container_id);
            } else {
                int wstatus;
                waitpid(pid, &wstatus, 0);
                int code = WIFEXITED(wstatus) ? WEXITSTATUS(wstatus)
                                              : 128 + WTERMSIG(wstatus);
                resp.status = code;
                snprintf(resp.message, CONTROL_MESSAGE_LEN,
                         "container '%s' finished exit_code=%d\n",
                         req.container_id, code);
            }
            break;
        }

        case CMD_PS:
            handle_ps(&ctx, &resp);
            break;

        case CMD_LOGS:
            handle_logs(&ctx, &req, &resp);
            break;

        case CMD_STOP:
            handle_stop(&ctx, &req, &resp);
            break;

        default:
            resp.status = -1;
            snprintf(resp.message, CONTROL_MESSAGE_LEN,
                     "ERROR: unknown command %d\n", req.kind);
            break;
        }

        /* Send response back to CLI client */
        send(client_fd, &resp, sizeof(resp), 0);
        close(client_fd);
    }

    fprintf(stderr, "[supervisor] shutting down\n");

    /* --- Orderly shutdown --- */

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give containers a moment to exit, then drain remaining children */
    sleep(1);
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;

    /* Shut down the logging pipeline and join the logger thread */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    fprintf(stderr, "[supervisor] logger thread joined\n");

cleanup:
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free container metadata list */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *cur = ctx.containers;
    while (cur) {
        container_record_t *next = cur->next;
        free(cur);
        cur = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
    }

    fprintf(stderr, "[supervisor] clean exit\n");
    g_ctx = NULL;
    return 0;
}

/* ---------------------------------------------------------------
 * CLI client: send a control request to the supervisor
 * --------------------------------------------------------------- */

/*
 * send_control_request - Connect to the supervisor socket, send the request,
 * receive the response, and print it.
 *
 * This is the "client side" of Path B (control channel).  Each CLI command
 * (start, ps, logs, stop) calls this function after filling in a
 * control_request_t.  The function connects to the supervisor's UNIX domain
 * socket, sends the struct, waits for a control_response_t, prints the
 * message, and returns the status code to the shell.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s: %s\n"
                        "Is the supervisor running? Try: sudo ./engine supervisor <rootfs>\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    /* Send request */
    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    /* Receive response */
    if (recv(fd, &resp, sizeof(resp), MSG_WAITALL) != (ssize_t)sizeof(resp)) {
        perror("recv");
        close(fd);
        return 1;
    }

    close(fd);

    /* Print the supervisor's reply */
    printf("%s", resp.message);
    return (resp.status == 0) ? 0 : 1;
}

/* ---------------------------------------------------------------
 * CLI command handlers (thin wrappers around send_control_request)
 * --------------------------------------------------------------- */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

/* ---------------------------------------------------------------
 * main
 * --------------------------------------------------------------- */

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}