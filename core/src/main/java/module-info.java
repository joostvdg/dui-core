module com.github.joostvdg.dui.core {
    requires com.github.joostvdg.dui.api;
    requires com.github.joostvdg.dui.logging;
    requires com.github.joostvdg.dui.server;

    uses com.github.joostvdg.dui.logging.Logger;
    uses com.github.joostvdg.dui.server.api.DuiServer;
}
