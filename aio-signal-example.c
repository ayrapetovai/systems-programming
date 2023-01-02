// linux: gcc -O2 -o aio-signal-example aio-signal-example.c -lrt
// macOS: gcc -O2 -o aio-signal-example aio-signal-example.c

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#ifdef __linux__
#include <aio.h>
#include <time.h>
#else
#include <sys/aio.h>
#endif
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/mman.h>

/*
[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -s -b1024 ./testfile4mb
reading sync from file ./testfile4mb, size 4194304 bytes, buffer size 1024 bytes
all idle iterations count 0, non-event iterations count 0
aio_read count 0, aio_return count 0
aio_read to (by signal IO ready) aio_return average latency nan ns
time spent for all aio_read 0 ns, all aio_return 0 ns, summary for all time 0 ns
average time spent for each aio_read nan ns, each aio_return nan ns, average each (aio_read + aio_return) time nan ns
pread count 4097
average time spent for pread 1267.268733 ns

real	0m0,010s
user	0m0,004s
sys	    0m0,006s

[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -a -b1024 ./testfile4mb
reading async from file ./testfile4mb, size 4194304 bytes, buffer size 1024 bytes
all idle iterations count 10774812, non-event iterations count 10774812
aio_read count 4097, aio_return count 4097
aio_read to (by signal IO ready) aio_return average latency 12921.386719 ns
time spent for all aio_read 6095000 ns, all aio_return 3359000 ns, summary for all time 9454000 ns
average time spent for each aio_read 1487.673908 ns, each aio_return 819.868196 ns, average each (aio_read + aio_return) time 2307.542104 ns
pread count 0
average time spent for pread nan ns

real	0m0,058s
user	0m0,032s
sys 	0m0,025s
 */

/*
[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -s -b$((4*1024*1024)) ./testfile4mb
reading sync from file ./testfile4mb, size 4194304 bytes, buffer size 4194304 bytes
all idle iterations count 0, non-event iterations count 0
aio_read count 0, aio_return count 0
aio_read to (by signal IO ready) aio_return average latency nan ns
time spent for all aio_read 0 ns, all aio_return 0 ns, summary for all time 0 ns
average time spent for each aio_read nan ns, each aio_return nan ns, average each (aio_read + aio_return) time nan ns
pread count 2
average time spent for pread 1937500.000000 ns

real	0m0,009s
user	0m0,002s
sys	    0m0,005s
[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -a -b$((4*1024*1024)) ./testfile4mb
reading async from file ./testfile4mb, size 4194304 bytes, buffer size 4194304 bytes
all idle iterations count 991368, non-event iterations count 991368
aio_read count 2, aio_return count 2
aio_read to (by signal IO ready) aio_return average latency 2392000.000000 ns
time spent for all aio_read 13000 ns, all aio_return 2000 ns, summary for all time 15000 ns
average time spent for each aio_read 6500.000000 ns, each aio_return 1000.000000 ns, average each (aio_read + aio_return) time 7500.000000 ns
pread count 0
average time spent for pread nan ns

real	0m0,008s
user	0m0,004s
sys 	0m0,002s
*/

/*
[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -s -b$((4*1024*1024)) ./testfile40mb
reading sync from file ./testfile40mb, size 41943040 bytes, buffer size 4194304 bytes
all idle iterations count 0, non-event iterations count 0
aio_read count 0, aio_return count 0
aio_read to (by signal IO ready) aio_return average latency nan ns
time spent for all aio_read 0 ns, all aio_return 0 ns, summary for all time 0 ns
average time spent for each aio_read nan ns, each aio_return nan ns, average each (aio_read + aio_return) time nan ns
pread count 11
average time spent for pread 764363.636364 ns

real	0m0,014s
user	0m0,002s
sys 	0m0,011s
[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -a -b$((4*1024*1024)) ./testfile40mb
reading async from file ./testfile40mb, size 41943040 bytes, buffer size 4194304 bytes
all idle iterations count 3340460, non-event iterations count 3340460
aio_read count 11, aio_return count 11
aio_read to (by signal IO ready) aio_return average latency 840500.000000 ns
time spent for all aio_read 26000 ns, all aio_return 11000 ns, summary for all time 37000 ns
average time spent for each aio_read 2363.636364 ns, each aio_return 1000.000000 ns, average each (aio_read + aio_return) time 3363.636364 ns
pread count 0
average time spent for pread nan ns

real	0m0,014s
user	0m0,010s
sys	    0m0,003s
*/

/*
[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -s -b$((4*1024*1024)) ./testfile4gb
reading sync from file ./testfile4gb, size 4194304000 bytes, buffer size 4194304 bytes
all idle iterations count 0, non-event iterations count 0
aio_read count 0, aio_return count 0
aio_read to (by signal IO ready) aio_return average latency nan ns
time spent for all aio_read 0 ns, all aio_return 0 ns, summary for all time 0 ns
average time spent for each aio_read nan ns, each aio_return nan ns, average each (aio_read + aio_return) time nan ns
pread count 1001
average time spent for pread 794794.205794 ns

real	0m0,801s
user	0m0,004s
sys 	0m0,795s
[~/Desktop/Exercises/cpp/kqueue (main)]
$ time ./aio-signal-example -a -b$((4*1024*1024)) ./testfile4gb
reading async from file ./testfile4gb, size 4194304000 bytes, buffer size 4194304 bytes
all idle iterations count 255437648, non-event iterations count 255437648
aio_read count 1001, aio_return count 1001
aio_read to (by signal IO ready) aio_return average latency 626810.000000 ns
time spent for all aio_read 2302000 ns, all aio_return 1026000 ns, summary for all time 3328000 ns
average time spent for each aio_read 2299.700300 ns, each aio_return 1024.975025 ns, average each (aio_read + aio_return) time 3324.675325 ns
pread count 0
average time spent for pread nan ns

real	0m0,632s
user	0m0,617s
sys 	0m0,012s
*/


static const int io_ready_signal_number = SIGIO;

static volatile sig_atomic_t need_to_check_read_event = 1;

static volatile sig_atomic_t passing_sig_val_ptr_works = 0;

static volatile sig_atomic_t file_descriptor = -1;

static struct {
    long program_start_time_ns;

    long idle_iterations_io_not_ready_count;
    long idle_iteration_without_event;

    long synchronous_read_time_ns;
    long synchronous_read_count;

    long asynchronous_read_time_ns;
    long asynchronous_read_count;
    long asynchronous_read_latency_start_ns;
    long asynchronous_latency_time_ns;
    long asynchronous_latency_count;

    long asynchronous_return_time_ns;
    long asynchronous_return_count;
} statistics = {};

long stop_watch_ns() {
    struct timespec time_now= {0};
#ifdef __linux__
    clock_gettime(CLOCK_BOOTTIME, &time_now);
#else
    clock_gettime(CLOCK_REALTIME, &time_now);
#endif
    return time_now.tv_sec * 1000 * 1000 * 1000 + time_now.tv_nsec;
}

long end_watch_ns(const long start_ns) {
    return stop_watch_ns() - start_ns;
}

// ret: 1 - done, 0 - not done
int synchronous_read(struct aiocb *aio_request, char do_output) {
    long start_ns = stop_watch_ns();

    // time spent for pread from 2 to 5 or 18 micro seconds, with buffer 4096 bytes
    // time spent for pread from 0 to 1 micro seconds, with buffer 4 bytes
    // time spent for pread ~300 micro seconds for big files with big buffers
    ssize_t bytes_read = pread(aio_request->aio_fildes, (void *) aio_request->aio_buf, aio_request->aio_nbytes, aio_request->aio_offset);

    statistics.synchronous_read_time_ns += end_watch_ns(start_ns);
    statistics.synchronous_read_count++;

    if (bytes_read == 0) {
        // nothing was read, end of file reached
        return 1;
    } else if (bytes_read > 0) {
        if (do_output) {
            printf("pread done, bytes read %ld\n", bytes_read);
            char str[bytes_read + 1];
            bzero(str, bytes_read + 1);
            memcpy(str, (const void *) aio_request->aio_buf, bytes_read);
            printf("buffer: '%s'\n", str);
        }
        aio_request->aio_offset += bytes_read;
    } else {
        perror("pread failed");
    }
    return 0;
}

// ret: 1 - done, 0 - not done
int asynchronous_read(struct aiocb *aio_request, char do_output) {
    static int read_operation_submitted = 0;

    if (!read_operation_submitted) {
        // set flag of necessarily to check IO result
        // before potential SIGIO could come
        need_to_check_read_event = 0;

        statistics.asynchronous_read_latency_start_ns = stop_watch_ns();

        // time spent for aio_read from 1 to 2 micro seconds
        const int request_submit_status = aio_read(aio_request);

        statistics.asynchronous_read_time_ns += end_watch_ns(statistics.asynchronous_read_latency_start_ns);
        statistics.asynchronous_read_count++;

        if (request_submit_status == 0) {
            read_operation_submitted = 1;
        } else {
            perror("aio_read failed");
            return 1;
        }
    } else {
        long start_ns = stop_watch_ns();

        // time spent for aio_return from 0 to 1 micro seconds
        const ssize_t bytes_read = aio_return(aio_request);

        statistics.asynchronous_return_time_ns += end_watch_ns(start_ns);
        statistics.asynchronous_return_count++;

        if (bytes_read > 0) {
            statistics.asynchronous_latency_time_ns += end_watch_ns(statistics.asynchronous_read_latency_start_ns);
            statistics.asynchronous_latency_count++;

            if (do_output) {
                printf("aio_read done, bytes read %ld\n", bytes_read);
                char str[bytes_read + 1];
                bzero(str, bytes_read + 1);
                memcpy(str, (const void *) aio_request->aio_buf, bytes_read);
                printf("buffer: '%s'\n", str);
            }

            aio_request->aio_offset += bytes_read;

            read_operation_submitted = 0;
        } else if (bytes_read == 0) {
            return 1;
        } else if (errno == EINPROGRESS) {
            // control does not reach tish code because of signal notification
            // never notifies about IO when it is not ready
            statistics.idle_iterations_io_not_ready_count++;
        } else {
            perror("aio_return gaven an error");
            return 1;
        }
    }
    return 0;
}

void print_stats() {
    printf("idle iterations count: IO not ready  %ld, no notification %ld\n", statistics.idle_iterations_io_not_ready_count, statistics.idle_iteration_without_event);

    double aio_read_avg_time = (double) statistics.asynchronous_read_time_ns / (double) statistics.asynchronous_read_count;
    double aio_return_avg_time = (double) statistics.asynchronous_return_time_ns / (double) statistics.asynchronous_return_count;
    double aio_latency = (double) statistics.asynchronous_latency_time_ns / (double) statistics.asynchronous_latency_count;

    printf("aio_read count %ld, aio_return count %ld\n", statistics.asynchronous_read_count, statistics.asynchronous_return_count);
    printf("aio_read to (by signal IO ready) aio_return average latency %lf ns\n", aio_latency);
    printf("time spent for all aio_read %ld ns, all aio_return %ld ns, summary for all time %ld ns\n",
           statistics.asynchronous_read_time_ns, statistics.asynchronous_return_time_ns,
           statistics.asynchronous_read_time_ns + statistics.asynchronous_return_time_ns);
    printf("average time spent for each aio_read %lf ns, each aio_return %lf ns, average each (aio_read + aio_return) time %lf ns\n", aio_read_avg_time, aio_return_avg_time, aio_read_avg_time + aio_return_avg_time);

    double pread_time = (double) statistics.synchronous_read_time_ns / (double) statistics.synchronous_read_count;

    printf("all pread count %ld\n", statistics.synchronous_read_count);
    printf("average time spent for pread %lf ns\n", pread_time);

	printf("passing sig_val works: %s\n", passing_sig_val_ptr_works? "yes": "no");
    printf("took %ld ns\n", end_watch_ns(statistics.program_start_time_ns));
}

void aio_notification_handler(__attribute__((unused)) int signal_value, siginfo_t* signal_info, void* context) {
    if (need_to_check_read_event) {
        printf("missed a handling of signal");
        exit(1);
    }
    need_to_check_read_event = 1;

    // if (signal_info->si_pid == getpid() && signal_info->si_code == SI_ASYNCIO) {
    if (signal_info != NULL && signal_info->si_value.sival_ptr != NULL) {
        struct aiocb* aio_request = signal_info->si_value.sival_ptr;
        if (aio_request != NULL) {
            passing_sig_val_ptr_works = aio_request->aio_fildes == file_descriptor;
        }
    }
    // do notify some logic to call aio_return on particular aio_request
    // instead of notifiing logic by flag, wich does not know an what aio_request do aio_return
}

void interrupt_signal_handler(int signal_value) {
    printf("\nexit by interruption signal %d\n", signal_value);
    print_stats();
    exit(1);
}

void print_help(const char* program_name_raw) {
    char* p = (char*) program_name_raw;
    char *last_slash = (char*) program_name_raw;
    for (; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    size_t program_name_length = strlen(program_name_raw) - (last_slash + 1 - program_name_raw);
    char program_name[program_name_length + 1];
    bzero(program_name, program_name_length + 1);
    strncpy(program_name, last_slash + 1, program_name_length);

    printf("Usage: %s [-s|-a] [-l] [-bBUFFER_SIZE] FILENAME\n", program_name);
    printf("\n");
    printf("Options: \n");
    printf("\t-s\t\tuse synchronous IO (open/pread), optional, unset by default\n");
    printf("\t-a\t\tuse asynchronous IO and signal IO notifications (open/aio_read/aio_return), optional, set by default\n");
    printf("\t-l\t\tlog read bytes as it was a plain text, optional, unset by default\n");
    printf("\t-bBUFFER_SIZE\tuse read buffer of size BUFFER_SIZE, optional, set to fs IO block size by default\n");
    printf("\tFILENAME\tfile name for reading, mandatory\n");
    printf("\n");
    printf("Examples:\n");
    printf("\t%s -s -b$((2*1024*1024)) ./testfile\n", program_name);
}

int main(int argc, char** argv) {
    statistics.program_start_time_ns = stop_watch_ns();

    char is_async = 1;
    char do_output = 0;
    size_t read_buffer_max_size = -1;

    int argument_counter = 1;
    if (argc > 1) {
        char found_dash_a = 0;
        char found_dash_s = 0;
        for (; argument_counter < argc; argument_counter++) {
            int i = argument_counter;
            if (strcmp(argv[i], "--help") == 0 ||
                strcmp(argv[i], "help") == 0 ||
                strcmp(argv[i], "-h") == 0 ||
                strcmp(argv[i], "h") == 0
            ) {
                print_help(argv[0]);
                exit(1);
            } else if (strcmp(argv[i], "-a") == 0) {
                if (found_dash_s) {
                    print_help(argv[0]);
                    exit(1);
                }
                found_dash_a = 1;
                is_async = 1;
            } else if (strcmp(argv[i], "-s") == 0) {
                if (found_dash_a) {
                    print_help(argv[0]);
                    exit(1);
                }
                found_dash_s = 1;
                is_async = 0;
            } else if (strcmp(argv[i], "-l") == 0) {
                do_output = 1;
            } else if (strncmp(argv[i], "-b", strlen("-b")) == 0) {
                if (strlen(argv[i]) > 2) {
                    char *begin = argv[i] + 2;
                    char *end;
                    read_buffer_max_size = strtol(begin, &end, 10);
                    if (begin == end) {
                        print_help(argv[0]);
                        exit(1);
                    }
                } else {
                    print_help(argv[0]);
                    exit(1);
                }
            } else {
                break;
            }
        }
    } else {
        print_help(argv[0]);
        exit(1);
    }

    if (argument_counter >= argc) {
        print_help(argv[0]);
        exit(2);
    }

    const size_t filename_length = strlen(argv[argc - 1]);
    char file_name_mutable[filename_length + 1];
    bzero(file_name_mutable, filename_length + 1);
    memcpy(file_name_mutable, argv[argc - 1], filename_length);
    const char *file_name = (const char*) file_name_mutable;

    struct stat file_stats;
    if (stat(file_name, &file_stats)) {
        perror("stat");
        exit(1);
    }

    file_descriptor = open(file_name, O_RDONLY);
    assert(file_descriptor != -1);

    if (read_buffer_max_size == -1) {
        struct statvfs file_system_info;
        if (fstatvfs(file_descriptor, &file_system_info) != 0) {
            perror("fstatvfs failed to get FS block size");
            exit(1);
        }
        read_buffer_max_size = file_system_info.f_bsize;
    }

    printf("reading %s from file %s, size %ld bytes, buffer size %zu bytes\n",
           (is_async? "async": "sync"), file_name, file_stats.st_size, read_buffer_max_size);

    struct sigaction signal_settings = { NULL, SA_ONSTACK | SA_RESTART | SA_SIGINFO, SIG_BLOCK };
    signal_settings.sa_sigaction = aio_notification_handler;
    if (-1 == sigaction(io_ready_signal_number, &signal_settings, NULL)) {
        perror("set signal aio ready");
        exit(1);
    }

    if (SIG_ERR == signal(SIGINT, interrupt_signal_handler)) {
        perror("set signal interrupt");
        exit(1);
    }

    char *read_buffer = malloc(read_buffer_max_size);

    struct aiocb *aio_request = malloc(sizeof(struct aiocb));
    memset(aio_request, 0, sizeof(struct aiocb));

    aio_request->aio_fildes = file_descriptor;
    aio_request->aio_offset = 0;
    aio_request->aio_buf = read_buffer;
    aio_request->aio_nbytes = read_buffer_max_size; // Length of transfer
    aio_request->aio_reqprio = 0;
    //aio_request->aio_lio_opcode = LIO_READ; // ignored by `aio_read()`
    aio_request->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
    aio_request->aio_sigevent.sigev_signo = io_ready_signal_number;
    // Mac OS X does not support real time signals ([RTS] in the posix specification),
    // which is the section of POSIX which adds userdata pointers to signals.

    // on macOS this will not pass a pointer, it will pass a NULL
    aio_request->aio_sigevent.sigev_value.sival_ptr = aio_request;

    // kqueue notification by aio is not implemented
    // thread notification by aio (function call) is not implemented
    // signal notification by aio with value is not implemented

    // only signal notification by aio without value is implemented

    for (;;) {
        if (is_async) {
            if (need_to_check_read_event) {
                if (asynchronous_read(aio_request, do_output)) {
                    break;
                }
            } else {
                statistics.idle_iteration_without_event++;
            }
        } else {
            if (synchronous_read(aio_request, do_output)) {
                break;
            }
        }
    }

    free(read_buffer);
    free(aio_request);
    close(file_descriptor);

    print_stats();
}
