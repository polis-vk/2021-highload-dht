package ru.mail.polis.service.exceptions;

public interface HttpException {
    String description();

    String httpCode();
}
