package com.github.cliffdurden.pessimisticlocksdemo.service;

import com.github.cliffdurden.pessimisticlocksdemo.entity.Book;
import com.github.cliffdurden.pessimisticlocksdemo.repository.BookRepository;
import jakarta.persistence.EntityManager;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CountDownLatch;

@Slf4j
@Service
@AllArgsConstructor
public class BookServiceImpl {

    private final BookRepository repository;

    private final EntityManager em;

    @SneakyThrows
    @Transactional
    public Book findByIsbnPessimisticReadT1(CountDownLatch latch, CountDownLatch latchAux, String isbn) {
        var result = repository.findByIsbnPessimisticRead(isbn).orElseThrow();
        latchAux.countDown();
        latch.await();
        return result;
    }

    @SneakyThrows
    @Transactional
    public Book findByIsbnPessimisticWriteT1(CountDownLatch latch, CountDownLatch latchAux, String isbn) {
        var result = repository.findByIsbnPessimisticWriteNoWait(isbn).orElseThrow();
        latchAux.countDown();
        latch.await();
        return result;
    }

    @SneakyThrows
    @Transactional
    public Book findByIsbnPessimisticWriteNoWaitLockT2(CountDownLatch latch, String isbn) {
        try {
            return repository.findByIsbnPessimisticWriteNoWait(isbn).orElseThrow();
        } finally {
            latch.countDown();
        }
    }

    @SneakyThrows
    @Transactional
    public Book findByIsbnPessimisticWriteSkipLockedT2(CountDownLatch latch, String isbn) {
        try {
            return repository.findByIsbnPessimisticWriteLockSkipLocked(isbn).orElse(null);
        } finally {
            latch.countDown();
        }
    }

    @SneakyThrows
    @Transactional(timeout = 1)
    public Book findByIsbnPessimisticWriteTransactionTimeoutT2(CountDownLatch latch, String isbn) {
        try {
            return repository.findByIsbnPessimisticWriteLockSkipLocked(isbn).orElse(null);
        } finally {
            latch.countDown();
        }
    }


    @SneakyThrows
    @Transactional
    public Book findByIsbnWithoutLockingT1(CountDownLatch latch, CountDownLatch latchAux, String isbn) {
        var result = repository.findByIsbnWithoutLock(isbn).orElseThrow();
        latchAux.countDown();
        latch.await();
        return result;
    }

    @Transactional(timeout = 1)
    public void setRatingByIsbnWithTransactionTimeoutT2(CountDownLatch latchT1, String isbn, Integer rating) {
        try {
            repository.setRatingByIsbn(isbn, rating);
        } finally {
            latchT1.countDown();
        }
    }

    @Transactional
    public int setRatingByIsbnQueryTimeoutT2(CountDownLatch latchT1, String isbn, Integer rating) {
        try {
            return repository.setRatingByIsbnQueryTimeout(isbn, rating);
        } finally {
            latchT1.countDown();
        }
    }

    public void save(Book book) {
        repository.save(book);
    }

    public void deleteAll() {
        repository.deleteAll();
    }


}
