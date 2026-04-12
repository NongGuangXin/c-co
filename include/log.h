#pragma once

#include <concepts>
#include <format>
#include <source_location>
#include <string_view>

class Fmt final {
    using sloc = std::source_location;

  public:
    template <typename T>
    requires std::constructible_from<std::string_view, T>
    Fmt(T fmt, const sloc& loc = sloc::current()): _fmt(fmt), _loc(loc) { }

    size_t line() noexcept;
    std::string_view file_name() noexcept;
    std::string_view str() noexcept;

  private:
    std::string_view _fmt;
    const std::source_location _loc;
};

class log {
  public:
    enum class Level : unsigned int { DBUG = 0, INFO, WARN, ERRO, STOP };

  public:
    static void dbug(Fmt fmt, const auto&... args) {
        if(__impl().check(Level::DBUG) == false) { return; }

        std::string str =
            std::format("[in {}:{}] ", fmt.file_name(), fmt.line());

        str += std::vformat(fmt.str(), std::make_format_args(args...));
        __impl().write(Level::DBUG, str);
    }

    static void info(Fmt fmt, const auto&... args) {
        if(__impl().check(Level::INFO) == false) { return; }

        std::string str =
            std::vformat(fmt.str(), std::make_format_args(args...));
        __impl().write(Level::INFO, str);
    }

    static void warn(Fmt fmt, const auto&... args) {
        if(__impl().check(Level::WARN) == false) { return; }

        std::string str =
            std::vformat(fmt.str(), std::make_format_args(args...));
        __impl().write(Level::WARN, str);
    }

    static void erro(Fmt fmt, const auto&... args) {
        if(__impl().check(Level::ERRO) == false) { return; }

        std::string str =
            std::format("[in {}:{}] ", fmt.file_name(), fmt.line());

        str += std::vformat(fmt.str(), std::make_format_args(args...));
        __impl().write(Level::ERRO, str);
    }

    static void flush() {
        __impl().flush();
    };

  public:
    static void set_level(Level level) {
        __impl().set_level(level);
    }

    static Level get_level() {
        return __impl().get_level();
    }

  private:
    log() { }
    ~log() { }

    struct impl {
        void write(Level level, std::string_view str);
        void flush();

        bool check(Level level);
        Level get_level();
        void set_level(Level level);

        impl();
    };

    static impl& __impl() {
        static impl log;
        return log;
    }
};