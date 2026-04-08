#include "log.h"
#include <atomic>
#include <string_view>

#ifdef USE_SPDLOG
#include "spdlog/common.h"
#include "spdlog/spdlog.h"
#else
#include <chrono>
#include <iostream>
#endif

std::string_view Fmt::file_name() noexcept {
    std::string_view sv{_loc.file_name()};
    size_t pos = sv.find_last_of('/');
    if(pos == std::string_view::npos) return sv;
    return sv.substr(pos + 1);
}

size_t Fmt::line() noexcept {
    return _loc.line();
}

std::string_view Fmt::str() noexcept {
    return _fmt;
}

using Level                           = log::Level;
static std::atomic<Level> G_LOG_LEVEL = Level::INFO;

log::impl::impl() {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(nullptr);
#ifdef USE_SPDLOG
    spdlog::set_level(spdlog::level::debug);
#endif
}

#ifdef USE_SPDLOG
void log::impl::write(Level level, std::string_view str) {
    using func = void (*)(const std::basic_string_view<char>& msg);
    static constexpr std::array<func, 4> functable = {
        spdlog::debug, spdlog::info, spdlog::warn, spdlog::error};
    if(static_cast<size_t>(level) >= functable.size()) { return; }

    size_t idx = static_cast<size_t>(level);
    functable[idx](str);
}
#else
std::string format_timestamp() {
    auto now        = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto ms         = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;

    std::tm tm_buf{};
    localtime_r(&time_t_now, &tm_buf);

    std::ostringstream oss;
    oss << '[' << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S") << '.'
        << std::setfill('0') << std::setw(3) << ms.count() << "] ";

    return oss.str();
}

const std::string_view level_to_string(Level level) {
    static constexpr std::array<std::string_view, 4> level_str = {
        "dbug", "info", "warn", "erro"};

    return level_str[static_cast<size_t>(level)];
}

void log::impl::write(Level level, std::string_view str) {
    std::cout << format_timestamp() << "[" << level_to_string(level) << "] "
              << str << "\n";
    std::cout.flush();
}
#endif

bool log::impl::check(Level level) {
    return (level >= G_LOG_LEVEL.load(std::memory_order_relaxed));
}

Level log::impl::get_level() {
    return G_LOG_LEVEL.load(std::memory_order_relaxed);
}

void log::impl::set_level(Level level) {
    G_LOG_LEVEL.store(level, std::memory_order_relaxed);
}
