package net.explorviz.trace.persistence;

/**
 * Thrown if a record could not be persisted.
 */
public class PersistingException extends RuntimeException {

  private static final long serialVersionUID = 5379259515382372634L; // NOPMD

  public PersistingException() {
    super();
  }

  public PersistingException(final String message) {
    super(message);
  }

  public PersistingException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public PersistingException(final Throwable cause) {
    super(cause);
  }

  public PersistingException(final String message, final Throwable cause,
      final boolean enableSuppression,
      final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}

