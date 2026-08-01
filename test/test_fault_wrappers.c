#include <errno.h>
#include <p101_env/env.h>
#include <p101_error/error.h>
#include <p101_sync/sync.h>
#include <stdio.h>
#include <stdlib.h>

static int failures;

#define EXPECT(condition)                                                                                                                                                                                                                                          \
    do                                                                                                                                                                                                                                                             \
    {                                                                                                                                                                                                                                                              \
        if(!(condition))                                                                                                                                                                                                                                           \
        {                                                                                                                                                                                                                                                          \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #condition);                                                                                                                                                                                   \
            failures++;                                                                                                                                                                                                                                            \
        }                                                                                                                                                                                                                                                          \
    } while(0)

struct fault_state
{
    int checks;
    int errnum;
};

static int fail_next_call(const struct p101_env *env, const char *call_name, void *user_data)
{
    struct fault_state *state;

    (void)env;
    (void)call_name;
    state = user_data;
    state->checks++;
    return state->errnum;
}

/* P101_TEST_CASE(p101_pthread_cond_broadcast) */
static void test_p101_pthread_cond_broadcast(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL};
#else
    static const int errors[] = {EIO};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_cond_broadcast(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_cond_destroy) */
static void test_p101_pthread_cond_destroy(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EBUSY, EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EBUSY, EINVAL};
#else
    static const int errors[] = {EAGAIN, ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_cond_destroy(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_cond_init) */
static void test_p101_pthread_cond_init(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EINVAL, ENOMEM};
#else
    static const int errors[] = {EAGAIN, ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_cond_init(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_cond_signal) */
static void test_p101_pthread_cond_signal(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL};
#else
    static const int errors[] = {EIO};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_cond_signal(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_cond_timedwait) */
static void test_p101_pthread_cond_timedwait(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ETIMEDOUT};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, EPERM, ETIMEDOUT};
#else
    static const int errors[] = {EAGAIN, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM, ETIMEDOUT};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_cond_timedwait(env, err, NULL, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_cond_wait) */
static void test_p101_pthread_cond_wait(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#else
    static const int errors[] = {EAGAIN, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM, ETIMEDOUT};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_cond_wait(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_condattr_destroy) */
static void test_p101_pthread_condattr_destroy(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_condattr_destroy(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_condattr_getpshared) */
static void test_p101_pthread_condattr_getpshared(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_condattr_getpshared(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_condattr_init) */
static void test_p101_pthread_condattr_init(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_condattr_init(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_condattr_setpshared) */
static void test_p101_pthread_condattr_setpshared(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_condattr_setpshared(env, err, NULL, 0);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutex_destroy) */
static void test_p101_pthread_mutex_destroy(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EBUSY, EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EBUSY, EINVAL};
#else
    static const int errors[] = {EAGAIN, EINVAL, ENOMEM, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutex_destroy(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutex_getprioceiling) */
static void test_p101_pthread_mutex_getprioceiling(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#else
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutex_getprioceiling(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutex_init) */
static void test_p101_pthread_mutex_init(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EAGAIN, EINVAL, ENOMEM, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutex_init(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutex_lock) */
static void test_p101_pthread_mutex_lock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EDEADLK, EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD};
#else
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutex_lock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutex_setprioceiling) */
static void test_p101_pthread_mutex_setprioceiling(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#else
    static const int errors[] = {EAGAIN, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutex_setprioceiling(env, err, NULL, 0, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutex_trylock) */
static void test_p101_pthread_mutex_trylock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EBUSY, EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EBUSY, EINVAL, ENOTRECOVERABLE, EOWNERDEAD};
#else
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutex_trylock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutex_unlock) */
static void test_p101_pthread_mutex_unlock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, EPERM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, EPERM};
#else
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK, EINVAL, ENOTRECOVERABLE, EOWNERDEAD, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutex_unlock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_destroy) */
static void test_p101_pthread_mutexattr_destroy(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_destroy(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_getprioceiling) */
static void test_p101_pthread_mutexattr_getprioceiling(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_getprioceiling(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_getprotocol) */
static void test_p101_pthread_mutexattr_getprotocol(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL, ENOTSUP, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL, ENOTSUP, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_getprotocol(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_getpshared) */
static void test_p101_pthread_mutexattr_getpshared(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL, ENOTSUP};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_getpshared(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_gettype) */
static void test_p101_pthread_mutexattr_gettype(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_gettype(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_init) */
static void test_p101_pthread_mutexattr_init(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_init(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_setprioceiling) */
static void test_p101_pthread_mutexattr_setprioceiling(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_setprioceiling(env, err, NULL, 0);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_setprotocol) */
static void test_p101_pthread_mutexattr_setprotocol(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL, ENOTSUP, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL, ENOTSUP, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_setprotocol(env, err, NULL, 0);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_setpshared) */
static void test_p101_pthread_mutexattr_setpshared(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL, ENOTSUP};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_setpshared(env, err, NULL, 0);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_mutexattr_settype) */
static void test_p101_pthread_mutexattr_settype(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, ENOMEM};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_mutexattr_settype(env, err, NULL, 0);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_once) */
static void test_p101_pthread_once(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EIO};
#elif defined(__FreeBSD__)
    static const int errors[] = {EIO};
#else
    static const int errors[] = {EIO};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_once(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlock_destroy) */
static void test_p101_pthread_rwlock_destroy(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, ENOMEM, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EBUSY, EINVAL, EPERM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EBUSY, EINVAL, EPERM};
#else
    static const int errors[] = {EAGAIN, ENOMEM, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlock_destroy(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlock_init) */
static void test_p101_pthread_rwlock_init(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, ENOMEM, EPERM};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EBUSY, EINVAL, ENOMEM, EPERM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EBUSY, EINVAL, ENOMEM, EPERM};
#else
    static const int errors[] = {EAGAIN, ENOMEM, EPERM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlock_init(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlock_rdlock) */
static void test_p101_pthread_rwlock_rdlock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK, EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK, EINVAL, ENOMEM};
#else
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlock_rdlock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlock_tryrdlock) */
static void test_p101_pthread_rwlock_tryrdlock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK, EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK, EINVAL, ENOMEM};
#else
    static const int errors[] = {EAGAIN, EBUSY, EDEADLK};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlock_tryrdlock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlock_trywrlock) */
static void test_p101_pthread_rwlock_trywrlock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, EDEADLK};
#elif defined(__APPLE__)
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, ENOMEM};
#else
    static const int errors[] = {EBUSY, EDEADLK};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlock_trywrlock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlock_unlock) */
static void test_p101_pthread_rwlock_unlock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EIO};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL, EPERM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, EPERM};
#else
    static const int errors[] = {EIO};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlock_unlock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlock_wrlock) */
static void test_p101_pthread_rwlock_wrlock(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EBUSY, EDEADLK};
#elif defined(__APPLE__)
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {EBUSY, EDEADLK, EINVAL, ENOMEM};
#else
    static const int errors[] = {EBUSY, EDEADLK};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlock_wrlock(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlockattr_destroy) */
static void test_p101_pthread_rwlockattr_destroy(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {ENOMEM};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL};
#else
    static const int errors[] = {ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlockattr_destroy(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlockattr_getpshared) */
static void test_p101_pthread_rwlockattr_getpshared(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlockattr_getpshared(env, err, NULL, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlockattr_init) */
static void test_p101_pthread_rwlockattr_init(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {ENOMEM};
#elif defined(__APPLE__)
    static const int errors[] = {ENOMEM};
#elif defined(__FreeBSD__)
    static const int errors[] = {ENOMEM};
#else
    static const int errors[] = {ENOMEM};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlockattr_init(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_pthread_rwlockattr_setpshared) */
static void test_p101_pthread_rwlockattr_setpshared(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_pthread_rwlockattr_setpshared(env, err, NULL, 0);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_sem_close) */
static void test_p101_sem_close(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EACCES, EEXIST, EINTR, EINVAL, ENAMETOOLONG, ENFILE, ENOENT, ENOSPC};
#else
    static const int errors[] = {EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_sem_close(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_sem_open) */
static void test_p101_sem_open(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EACCES, EEXIST, EINVAL, EMFILE, ENAMETOOLONG, ENFILE, ENOENT, ENOMEM};
#elif defined(__APPLE__)
    static const int errors[] = {EACCES, EEXIST, EINTR, EINVAL, EMFILE, ENAMETOOLONG, ENFILE, ENOENT, ENOSPC};
#elif defined(__FreeBSD__)
    static const int errors[] = {EACCES, EEXIST, EINTR, EINVAL, ENAMETOOLONG, ENFILE, ENOENT, ENOSPC};
#else
    static const int errors[] = {EACCES, EEXIST, EINTR, EINVAL, EMFILE, ENAMETOOLONG, ENFILE, ENOENT, ENOMEM, ENOSPC};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        sem_t *result = p101_sem_open(env, err, NULL, 0);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_sem_post) */
static void test_p101_sem_post(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EINVAL, EOVERFLOW};
#elif defined(__APPLE__)
    static const int errors[] = {EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EINVAL, EOVERFLOW};
#else
    static const int errors[] = {EINVAL, EOVERFLOW};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_sem_post(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_sem_trywait) */
static void test_p101_sem_trywait(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, EINTR, EINVAL, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EDEADLK, EINTR, EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EINTR, EINVAL};
#else
    static const int errors[] = {EAGAIN, EDEADLK, EINTR, EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_sem_trywait(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_sem_unlink) */
static void test_p101_sem_unlink(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EACCES, ENAMETOOLONG, ENOENT};
#elif defined(__APPLE__)
    static const int errors[] = {EACCES, ENAMETOOLONG, ENOENT};
#elif defined(__FreeBSD__)
    static const int errors[] = {EACCES, EEXIST, EINTR, EINVAL, ENAMETOOLONG, ENFILE, ENOENT, ENOSPC};
#else
    static const int errors[] = {EACCES, ENAMETOOLONG, ENOENT};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_sem_unlink(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

/* P101_TEST_CASE(p101_sem_wait) */
static void test_p101_sem_wait(struct p101_env *env, struct p101_error *err)
{
#ifdef __linux__
    static const int errors[] = {EAGAIN, EINTR, EINVAL, ETIMEDOUT};
#elif defined(__APPLE__)
    static const int errors[] = {EAGAIN, EDEADLK, EINTR, EINVAL};
#elif defined(__FreeBSD__)
    static const int errors[] = {EAGAIN, EINTR, EINVAL};
#else
    static const int errors[] = {EAGAIN, EDEADLK, EINTR, EINVAL};
#endif

    for(size_t index = 0U; index < sizeof(errors) / sizeof(errors[0]); index++)
    {
        struct fault_state state = {0, errors[index]};

        p101_env_set_fault_injector(env, fail_next_call, &state);
        int result = p101_sem_wait(env, err, NULL);
        (void)result;
        EXPECT(state.checks == 1);
        EXPECT(p101_error_is_errno(err, state.errnum));
        p101_error_reset(err);
    }
    p101_env_set_fault_injector(env, NULL, NULL);
}

int main(void)
{
    struct p101_error *err;
    struct p101_env   *env;

    err = p101_error_create(false);
    if(err == NULL)
    {
        return EXIT_FAILURE;
    }
    env = p101_env_create(err, NULL);
    if(env == NULL)
    {
        p101_error_destroy(err);
        return EXIT_FAILURE;
    }
    test_p101_pthread_cond_broadcast(env, err);
    test_p101_pthread_cond_destroy(env, err);
    test_p101_pthread_cond_init(env, err);
    test_p101_pthread_cond_signal(env, err);
    test_p101_pthread_cond_timedwait(env, err);
    test_p101_pthread_cond_wait(env, err);
    test_p101_pthread_condattr_destroy(env, err);
    test_p101_pthread_condattr_getpshared(env, err);
    test_p101_pthread_condattr_init(env, err);
    test_p101_pthread_condattr_setpshared(env, err);
    test_p101_pthread_mutex_destroy(env, err);
    test_p101_pthread_mutex_getprioceiling(env, err);
    test_p101_pthread_mutex_init(env, err);
    test_p101_pthread_mutex_lock(env, err);
    test_p101_pthread_mutex_setprioceiling(env, err);
    test_p101_pthread_mutex_trylock(env, err);
    test_p101_pthread_mutex_unlock(env, err);
    test_p101_pthread_mutexattr_destroy(env, err);
    test_p101_pthread_mutexattr_getprioceiling(env, err);
    test_p101_pthread_mutexattr_getprotocol(env, err);
    test_p101_pthread_mutexattr_getpshared(env, err);
    test_p101_pthread_mutexattr_gettype(env, err);
    test_p101_pthread_mutexattr_init(env, err);
    test_p101_pthread_mutexattr_setprioceiling(env, err);
    test_p101_pthread_mutexattr_setprotocol(env, err);
    test_p101_pthread_mutexattr_setpshared(env, err);
    test_p101_pthread_mutexattr_settype(env, err);
    test_p101_pthread_once(env, err);
    test_p101_pthread_rwlock_destroy(env, err);
    test_p101_pthread_rwlock_init(env, err);
    test_p101_pthread_rwlock_rdlock(env, err);
    test_p101_pthread_rwlock_tryrdlock(env, err);
    test_p101_pthread_rwlock_trywrlock(env, err);
    test_p101_pthread_rwlock_unlock(env, err);
    test_p101_pthread_rwlock_wrlock(env, err);
    test_p101_pthread_rwlockattr_destroy(env, err);
    test_p101_pthread_rwlockattr_getpshared(env, err);
    test_p101_pthread_rwlockattr_init(env, err);
    test_p101_pthread_rwlockattr_setpshared(env, err);
    test_p101_sem_close(env, err);
    test_p101_sem_open(env, err);
    test_p101_sem_post(env, err);
    test_p101_sem_trywait(env, err);
    test_p101_sem_unlink(env, err);
    test_p101_sem_wait(env, err);
    p101_env_destroy(env);
    p101_error_destroy(err);
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
