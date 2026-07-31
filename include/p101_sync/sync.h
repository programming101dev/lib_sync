#ifndef LIBP101_SYNC_SYNC_H
#define LIBP101_SYNC_SYNC_H

/*
 * Copyright 2026 D'Arcy Smith.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

#include <p101_env/env.h>
#include <p101_error/attributes.h>
#include <pthread.h>
#include <semaphore.h>

#ifdef __cplusplus
extern "C"
{
#endif

    int    p101_pthread_cond_broadcast(const struct p101_env *env, struct p101_error *err, pthread_cond_t *cond);
    int    p101_pthread_cond_destroy(const struct p101_env *env, struct p101_error *err, pthread_cond_t *cond);
    int    p101_pthread_cond_init(const struct p101_env *env, struct p101_error *err, pthread_cond_t *restrict cond, const pthread_condattr_t *restrict attr);
    int    p101_pthread_cond_signal(const struct p101_env *env, struct p101_error *err, pthread_cond_t *cond);
    int    p101_pthread_cond_timedwait(const struct p101_env *env, struct p101_error *err, pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex, const struct timespec *restrict abstime);
    int    p101_pthread_cond_wait(const struct p101_env *env, struct p101_error *err, pthread_cond_t *restrict cond, pthread_mutex_t *restrict mutex);
    int    p101_pthread_condattr_destroy(const struct p101_env *env, struct p101_error *err, pthread_condattr_t *attr);
    int    p101_pthread_condattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_condattr_t *restrict attr, int *restrict pshared);
    int    p101_pthread_condattr_init(const struct p101_env *env, struct p101_error *err, pthread_condattr_t *attr);
    int    p101_pthread_condattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_condattr_t *attr, int pshared);
    int    p101_pthread_mutex_destroy(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex);
    int    p101_pthread_mutex_getprioceiling(const struct p101_env *env, struct p101_error *err, const pthread_mutex_t *restrict mutex, int *restrict prioceiling);
    int    p101_pthread_mutex_init(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *restrict mutex, const pthread_mutexattr_t *restrict attr);
    int    p101_pthread_mutex_lock(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex);
    int    p101_pthread_mutex_setprioceiling(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *restrict mutex, int prioceiling, int *restrict old_ceiling);
    int    p101_pthread_mutex_trylock(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex);
    int    p101_pthread_mutex_unlock(const struct p101_env *env, struct p101_error *err, pthread_mutex_t *mutex);
    int    p101_pthread_mutexattr_destroy(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr);
    int    p101_pthread_mutexattr_getprioceiling(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict prioceiling);
    int    p101_pthread_mutexattr_getprotocol(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict protocol);
    int    p101_pthread_mutexattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict pshared);
    int    p101_pthread_mutexattr_gettype(const struct p101_env *env, struct p101_error *err, const pthread_mutexattr_t *restrict attr, int *restrict type);
    int    p101_pthread_mutexattr_init(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr);
    int    p101_pthread_mutexattr_setprioceiling(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int prioceiling);
    int    p101_pthread_mutexattr_setprotocol(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int protocol);
    int    p101_pthread_mutexattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int pshared);
    int    p101_pthread_mutexattr_settype(const struct p101_env *env, struct p101_error *err, pthread_mutexattr_t *attr, int type);
    int    p101_pthread_once(const struct p101_env *env, struct p101_error *err, pthread_once_t *once_control, void (*init_routine)(void));
    int    p101_pthread_rwlock_destroy(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock);
    int    p101_pthread_rwlock_init(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *restrict rwlock, const pthread_rwlockattr_t *restrict attr);
    int    p101_pthread_rwlock_rdlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock);
    int    p101_pthread_rwlock_tryrdlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock);
    int    p101_pthread_rwlock_trywrlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock);
    int    p101_pthread_rwlock_unlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock);
    int    p101_pthread_rwlock_wrlock(const struct p101_env *env, struct p101_error *err, pthread_rwlock_t *rwlock);
    int    p101_pthread_rwlockattr_destroy(const struct p101_env *env, struct p101_error *err, pthread_rwlockattr_t *attr);
    int    p101_pthread_rwlockattr_getpshared(const struct p101_env *env, struct p101_error *err, const pthread_rwlockattr_t *restrict attr, int *restrict pshared);
    int    p101_pthread_rwlockattr_init(const struct p101_env *env, struct p101_error *err, pthread_rwlockattr_t *attr);
    int    p101_pthread_rwlockattr_setpshared(const struct p101_env *env, struct p101_error *err, pthread_rwlockattr_t *attr, int pshared);
    int    p101_sem_close(const struct p101_env *env, struct p101_error *err, sem_t *sem);
    sem_t *p101_sem_open(const struct p101_env *env, struct p101_error *err, const char *name, int oflag, ...) P101_ATTR_WARN_UNUSED_RESULT;
    int    p101_sem_post(const struct p101_env *env, struct p101_error *err, sem_t *sem);
    int    p101_sem_trywait(const struct p101_env *env, struct p101_error *err, sem_t *sem);
    int    p101_sem_unlink(const struct p101_env *env, struct p101_error *err, const char *name);
    int    p101_sem_wait(const struct p101_env *env, struct p101_error *err, sem_t *sem);

#ifdef __cplusplus
}
#endif

#endif    // LIBP101_SYNC_SYNC_H
