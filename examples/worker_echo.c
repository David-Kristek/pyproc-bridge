/*
 * worker_echo.c -- C mirror of examples/worker_echo.py: a minimal
 * pyproc_bridge Worker. Connects back to a Supervisor over TCP, waits for
 * TASK, counts up to task["count"] sending an EVENT per step (responsive to
 * ABORT the whole time via select()), and sends exactly one terminal
 * message (RESULT or ABORTED) before closing. See pyproc_bridge/protocol.py
 * for the message contract this follows.
 *
 * Wire format (see pyproc_bridge/node.py): each message is a 4-byte
 * big-endian length prefix followed by that many bytes of UTF-8 JSON,
 * shaped {"event": "<name>", "data": <any>}.
 *
 * This file hand-rolls just enough JSON to speak that exact shape -- it is
 * NOT a general JSON parser/encoder, and it relies on this codebase always
 * emitting {"event":..., "data":...} in that key order. A real C
 * integration should use a small JSON library (e.g. cJSON) instead.
 *
 * Build (MinGW-w64):
 *     gcc -O2 -o worker_echo.exe worker_echo.c -lws2_32
 * Build (MSVC, "x64 Native Tools" prompt):
 *     cl worker_echo.c ws2_32.lib
 * Build (Linux/macOS, POSIX sockets):
 *     cc -O2 -o worker_echo worker_echo.c
 *
 * Run against examples/host_c.py, which spawns the compiled binary in
 * place of a Python worker:
 *     python -m examples.host_c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifdef _WIN32
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  pragma comment(lib, "ws2_32.lib")
   typedef SOCKET sock_t;
#  define CLOSESOCK closesocket
#  define SOCK_INVALID INVALID_SOCKET
#else
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <unistd.h>
#  include <errno.h>
   typedef int sock_t;
#  define CLOSESOCK close
#  define SOCK_INVALID (-1)
#endif

#define MAX_MSG_SIZE 4096  /* generous for this demo's tiny messages */

static void platform_init(void) {
#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
}

static void platform_cleanup(void) {
#ifdef _WIN32
    WSACleanup();
#endif
}

static sock_t connect_to_supervisor(const char *host, int port) {
    sock_t sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == SOCK_INVALID) return SOCK_INVALID;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        CLOSESOCK(sock);
        return SOCK_INVALID;
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof addr) != 0) {
        CLOSESOCK(sock);
        return SOCK_INVALID;
    }

    int one = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (const char *)&one, sizeof one);
    return sock;
}

/* Returns 1 on success, 0 on EOF/error. */
static int send_all(sock_t sock, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        int n = send(sock, buf + sent, (int)(len - sent), 0);
        if (n <= 0) return 0;
        sent += (size_t)n;
    }
    return 1;
}

static int recv_exact(sock_t sock, char *buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        int n = recv(sock, buf + got, (int)(len - got), 0);
        if (n <= 0) return 0;
        got += (size_t)n;
    }
    return 1;
}

/* Sends {"event": event, "data": json_data} where json_data is an
 * already-serialized JSON value (e.g. "true", "5", "\"a string\"",
 * "{\"step\":0}"). */
static int send_message(sock_t sock, const char *event, const char *json_data) {
    char payload[MAX_MSG_SIZE];
    int len = snprintf(payload, sizeof payload, "{\"event\":\"%s\",\"data\":%s}",
                        event, json_data);
    if (len < 0 || (size_t)len >= sizeof payload) return 0;

    unsigned char header[4];
    uint32_t be_len = htonl((uint32_t)len);
    memcpy(header, &be_len, 4);

    return send_all(sock, (const char *)header, 4) && send_all(sock, payload, (size_t)len);
}

/* Reads one framed message. event_out/data_out are NUL-terminated on
 * success. data_out holds the raw JSON text of the "data" field (relies on
 * this codebase always writing {"event":...,"data":...} in that order).
 * Returns 1 on success, 0 on clean EOF, -1 on error/oversized message. */
static int recv_message(sock_t sock, char *event_out, size_t event_cap,
                         char *data_out, size_t data_cap) {
    unsigned char header[4];
    if (!recv_exact(sock, (char *)header, 4)) return 0;
    uint32_t be_len;
    memcpy(&be_len, header, 4);
    uint32_t msg_len = ntohl(be_len);
    if (msg_len == 0 || msg_len >= MAX_MSG_SIZE) {
        fprintf(stderr, "[worker] rejecting message of %u bytes\n", (unsigned)msg_len);
        return -1;
    }

    char payload[MAX_MSG_SIZE];
    if (!recv_exact(sock, payload, msg_len)) return 0;
    payload[msg_len] = '\0';

    const char *event_key = strstr(payload, "\"event\":\"");
    const char *data_key = strstr(payload, "\"data\":");
    if (!event_key || !data_key) {
        fprintf(stderr, "[worker] malformed message: %s\n", payload);
        return -1;
    }

    const char *event_start = event_key + strlen("\"event\":\"");
    const char *event_end = strchr(event_start, '"');
    if (!event_end || (size_t)(event_end - event_start) >= event_cap) return -1;
    memcpy(event_out, event_start, (size_t)(event_end - event_start));
    event_out[event_end - event_start] = '\0';

    const char *data_start = data_key + strlen("\"data\":");
    size_t data_len = strlen(data_start);
    if (data_len == 0 || data_len - 1 >= data_cap) return -1;
    /* Drop the outer object's closing '}'. */
    memcpy(data_out, data_start, data_len - 1);
    data_out[data_len - 1] = '\0';

    return 1;
}

static void parse_task(const char *data, int *count, double *delay_s) {
    const char *p = strstr(data, "\"count\":");
    if (p) *count = atoi(p + strlen("\"count\":"));
    p = strstr(data, "\"delay_s\":");
    if (p) *delay_s = strtod(p + strlen("\"delay_s\":"), NULL);
}

/* Waits up to delay_s seconds for the next message, reacting to ABORT if it
 * arrives; returns after the delay elapses either way. */
static void wait_with_abort_check(sock_t sock, double delay_s, int *abort_requested) {
    struct timeval tv;
    tv.tv_sec = (long)delay_s;
    tv.tv_usec = (long)((delay_s - (double)tv.tv_sec) * 1000000.0);

    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(sock, &readfds);

    int rc = select((int)sock + 1, &readfds, NULL, NULL, &tv);
    if (rc > 0 && FD_ISSET(sock, &readfds)) {
        char event[32], data[MAX_MSG_SIZE];
        if (recv_message(sock, event, sizeof event, data, sizeof data) == 1 &&
            strcmp(event, "abort") == 0) {
            printf("[worker] abort requested\n");
            *abort_requested = 1;
        }
    }
}

int main(void) {
    platform_init();

    const char *port_env = getenv("IPC_PORT");
    int port = port_env ? atoi(port_env) : 5557;

    sock_t sock = connect_to_supervisor("127.0.0.1", port);
    if (sock == SOCK_INVALID) {
        fprintf(stderr, "[worker] could not connect to supervisor on port %d\n", port);
        platform_cleanup();
        return 1;
    }

    int abort_requested = 0;
    int count = 5;
    double delay_s = 0.5;
    int have_task = 0;

    /* Wait for TASK, honouring an ABORT that arrives first (mirrors
     * worker_echo.py's global abort_requested flag). */
    while (!have_task) {
        char event[32], data[MAX_MSG_SIZE];
        int rc = recv_message(sock, event, sizeof event, data, sizeof data);
        if (rc <= 0) goto cleanup;

        if (strcmp(event, "task") == 0) {
            parse_task(data, &count, &delay_s);
            have_task = 1;
        } else if (strcmp(event, "abort") == 0) {
            printf("[worker] abort requested\n");
            abort_requested = 1;
        } else {
            printf("[worker] Warning: unhandled event '%s'\n", event);
        }
    }

    for (int i = 0; i < count && !abort_requested; i++) {
        wait_with_abort_check(sock, delay_s, &abort_requested);
        if (abort_requested) break;

        char step_data[64];
        snprintf(step_data, sizeof step_data, "{\"step\":%d,\"of\":%d}", i, count);
        send_message(sock, "event", step_data);
    }

    if (abort_requested) {
        send_message(sock, "aborted", "true");
    } else {
        char result[64];
        snprintf(result, sizeof result, "\"counted to %d\"", count);
        send_message(sock, "result", result);
    }

cleanup:
    CLOSESOCK(sock);
    platform_cleanup();
    return 0;
}
