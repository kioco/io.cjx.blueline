package io.cjx.blueline.docutils;

public class PluginDocRuntimeException extends RuntimeException {

    public PluginDocRuntimeException() {
        super();
    }

    public PluginDocRuntimeException(String message) {
        super(message);
    }

    public PluginDocRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public PluginDocRuntimeException(Throwable cause) {
        super(cause);
    }
}
