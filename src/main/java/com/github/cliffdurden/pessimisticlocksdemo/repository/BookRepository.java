package com.github.cliffdurden.pessimisticlocksdemo.repository;


import com.github.cliffdurden.pessimisticlocksdemo.entity.Book;
import jakarta.persistence.*;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface BookRepository extends JpaRepository<Book, Long> {


    /**
     * <pre>
     * Hibernate:
     *     select
     *         b1_0.id,
     *         b1_0.author,
     *         b1_0.isbn,
     *         b1_0.rating,
     *         b1_0.title
     *     from
     *         book b1_0
     *     where
     *         b1_0.isbn=? <b>for share</b>
     * </pre>
     */
    @Query("select b from Book b where b.isbn = :isbn")
    @Lock(LockModeType.PESSIMISTIC_READ)
    Optional<Book> findByIsbnPessimisticRead(@Param("isbn") String isbn);

    /**
     * <pre>
     * Hibernate:
     *     select
     *         b1_0.id,
     *         b1_0.author,
     *         b1_0.isbn,
     *         b1_0.rating,
     *         b1_0.title
     *     from
     *         book b1_0
     *     where
     *         b1_0.isbn=? <b>for no key update nowait</b>
     * </pre>
     */
    @Query("select b from Book b where b.isbn = :isbn")
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = AvailableSettings.JAKARTA_LOCK_TIMEOUT, value = "0")})
    Optional<Book> findByIsbnPessimisticWriteNoWait(@Param("isbn") String isbn);

    /**
     * PostgreSQL not supported jakarta.persistence.lock.timeout. only skip locked can be tested
     * <pre>
     * Hibernate:
     *     select
     *         b1_0.id,
     *         b1_0.author,
     *         b1_0.isbn,
     *         b1_0.rating,
     *         b1_0.title
     *     from
     *         book b1_0
     *     where
     *         b1_0.isbn=? <b>for no key update skip locked</b>
     * </pre>
     */
    @Query("select b from Book b where b.isbn = :isbn")
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = AvailableSettings.JAKARTA_LOCK_TIMEOUT, value = "-2")})
    Optional<Book> findByIsbnPessimisticWriteLockSkipLocked(@Param("isbn") String isbn);


    /**
     * <pre>
     * Hibernate:
     *     select
     *         b1_0.id,
     *         b1_0.author,
     *         b1_0.isbn,
     *         b1_0.rating,
     *         b1_0.title
     *     from
     *         book b1_0
     *     where
     *         b1_0.isbn=?
     * </pre>
     */
    @Query("select b from Book b where b.isbn = :isbn")
    Optional<Book> findByIsbnWithoutLock(@Param("isbn") String isbn);


    @Modifying
    @Query(value = "update Book b set b.rating = :rating where b.isbn = :isbn")
    @QueryHints({@QueryHint(name = "jakarta.persistence.query.timeout", value = "1000")})
    int setRatingByIsbnQueryTimeout(@Param("isbn") String isbn, @Param("rating") Integer rating);


    @Modifying
    @Query(value = "update Book b set b.rating = :rating where b.isbn = :isbn")
    int setRatingByIsbn(@Param("isbn") String isbn, @Param("rating") Integer rating);
}