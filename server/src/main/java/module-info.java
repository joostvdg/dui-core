module com.github.joostvdg.dui.server {
    requires com.github.joostvdg.dui.api;
    requires com.github.joostvdg.dui.client;
    requires com.github.joostvdg.dui.logging;

    exports com.github.joostvdg.dui.server.api;
    uses com.github.joostvdg.dui.logging.Logger;
}
