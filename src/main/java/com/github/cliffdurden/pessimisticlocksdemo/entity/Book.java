package com.github.cliffdurden.pessimisticlocksdemo.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Builder
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Book {

    @Id
    @GeneratedValue
    private Long id;

    private String title;

    private String author;

    private Integer rating;

    private String isbn;

}
