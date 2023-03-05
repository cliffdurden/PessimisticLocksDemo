package com.github.cliffdurden.pessimisticlocksdemo;

import com.github.cliffdurden.pessimisticlocksdemo.entity.Book;
import com.github.cliffdurden.pessimisticlocksdemo.service.BookServiceImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.*;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.context.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.shaded.org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.CompletableFuture.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * T1 - transaction 1
 * T2 - transaction 2
 */
@Slf4j
@SpringBootTest
@Testcontainers
class PessimisticLocksDemoApplicationTests {

    @Container
    static final PostgreSQLContainer<?> POSTGRESQL_CONTAINER = new PostgreSQLContainer<>("postgres:15.2");

    @Autowired
    private BookServiceImpl testSubject;

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", POSTGRESQL_CONTAINER::getJdbcUrl);
        registry.add("DB_USERNAME", POSTGRESQL_CONTAINER::getUsername);
        registry.add("DB_PASSWORD", POSTGRESQL_CONTAINER::getPassword);
    }

    @BeforeEach
    void setUp() {
        testSubject.save(book1());
        testSubject.save(book2());
    }

    @AfterEach
    void tearDown() {
        testSubject.deleteAll();
    }

    @DisplayName("Test read lock with transaction timeout")
    @SneakyThrows
    @Test
    void testReadLockWithTransactionTimeout() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);

        runAsync(
                () -> testSubject.findByIsbnPessimisticReadT1(latchT1, latchT1aux, "9780321751041")
        );
        latchT1aux.await(); // wait until T1 has been started


        var exception = assertThrows(
                JpaSystemException.class,
                () -> testSubject.setRatingByIsbnWithTransactionTimeoutT2(latchT1, "9780321751041", 10),
                "org.hibernate.TransactionException was expected"
        );
        assertEquals("transaction timeout expired", exception.getMessage());
    }

    @DisplayName("Test read lock with query timeout")
    @SneakyThrows
    @Test
    void testReadLockWithJDbcTimeout() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);

        runAsync(
                () -> testSubject.findByIsbnPessimisticReadT1(latchT1, latchT1aux, "9780321751041")
        );
        latchT1aux.await(); // wait until T1 has been started

        var exception = assertThrows(
                QueryTimeoutException.class,
                () -> testSubject.setRatingByIsbnQueryTimeoutT2(latchT1, "9780321751041", 10),
                "org.springframework.dao.QueryTimeoutException was expected"
        );
        assertTrue(exception.getMessage().startsWith("JDBC exception executing SQL"));
    }

    @DisplayName("Test read without lock")
    @SneakyThrows
    @Test
    void testReadWithoutLock() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);

        var futureT1Result = supplyAsync(
                () -> testSubject.findByIsbnWithoutLockingT1(latchT1, latchT1aux, "9780321751041")
        );
        latchT1aux.await(); // wait until T1 has been started

        var rowsUpdated = testSubject.setRatingByIsbnQueryTimeoutT2(latchT1, "9780321751041", 10);

        assertEquals(1, rowsUpdated);
        assertEquals(0, futureT1Result.get().getRating());
    }

    @DisplayName("Test write lock")
    @SneakyThrows
    @Test
    void tesWriteLock() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);

        runAsync(
                () -> testSubject.findByIsbnPessimisticWriteT1(latchT1, latchT1aux, "9780321751041")
        );
        latchT1aux.await(); // wait until T1 has been started

        var exception = assertThrows(
                PessimisticLockingFailureException.class,
                () -> testSubject.findByIsbnPessimisticWriteNoWaitLockT2(latchT1, "9780321751041"),
                "org.springframework.dao.PessimisticLockingFailureException was expected"
        );
        assertEquals("ERROR: could not obtain lock on row in relation \"book\"", ExceptionUtils.getRootCause(exception).getMessage());
    }

    @DisplayName("Test write lock with lock timeout")
    @SneakyThrows
    @Test
    void tesWriteLockWithSkipLocked() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);

        runAsync(
                () -> testSubject.findByIsbnPessimisticWriteT1(latchT1, latchT1aux, "9780321751041")
        );
        latchT1aux.await(); // wait until T1 has been started

        var result = testSubject.findByIsbnPessimisticWriteSkipLockedT2(latchT1, "9780321751041");

        assertNull(result, "Should not find any locked record");
    }

    @DisplayName("Test write lock transaction timeout")
    @SneakyThrows
    @Test
    void tesWriteLockWithTransactionTimeout() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);

        runAsync(
                () -> testSubject.findByIsbnPessimisticWriteT1(latchT1, latchT1aux, "9780321751041")
        );
        latchT1aux.await(); // wait until T1 has been started

        var exception = assertThrows(
                JpaSystemException.class,
                () -> testSubject.findByIsbnPessimisticWriteTransactionTimeoutT2(latchT1, "9780321751041"),
                "org.hibernate.TransactionException was expected"
        );
        assertEquals("transaction timeout expired", exception.getMessage());
    }

    @DisplayName("Test write lock with with attempt to update with query timeout")
    @SneakyThrows
    @Test
    void tesWriteLockWithAttemptToUpdateWithQueryTimeout() {
        CountDownLatch latchT1 = new CountDownLatch(1);
        CountDownLatch latchT1aux = new CountDownLatch(1);

        runAsync(
                () -> testSubject.findByIsbnPessimisticWriteT1(latchT1, latchT1aux, "9780321751041")
        );
        latchT1aux.await(); // wait until T1 has been started

        var exception = assertThrows(
                QueryTimeoutException.class,
                () -> testSubject.setRatingByIsbnQueryTimeoutT2(latchT1, "9780321751041", 10),
                "org.springframework.dao.QueryTimeoutException was expected"
        );
        assertTrue(exception.getMessage().startsWith("JDBC exception executing SQL"));
    }

    private Book book1() {
        return Book.builder()
                .author("Donald Knuth")
                .title("Art of Computer Programming, Volume 1: Fundamental Algorithms")
                .rating(0)
                .isbn("9780321751041")
                .build();
    }

    private Book book2() {
        return Book.builder()
                .author("Donald Knuth")
                .title("Art of Computer Programming, Volume 2: Seminumerical Algorithms")
                .rating(5)
                .isbn("9780201896848")
                .build();
    }

}
