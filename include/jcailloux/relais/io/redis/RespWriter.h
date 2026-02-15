#ifndef JCX_RELAIS_IO_REDIS_RESP_WRITER_H
#define JCX_RELAIS_IO_REDIS_RESP_WRITER_H

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>

namespace jcailloux::relais::io {

// RespWriter — serializes Redis commands into RESP2 wire format.
//
// RESP2 command format:
//   *<argc>\r\n
//   $<len>\r\n<data>\r\n
//   ...
//
// Usage:
//   RespWriter w;
//   w.writeCommand(argc, argv, argvlen);
//   send(w.data(), w.size());
//   w.consume(bytes_sent);

class RespWriter {
public:
    void writeCommand(int argc, const char** argv, const size_t* argvlen) {
        // Pre-calculate total size to minimize allocations
        size_t total = 1 + numDigits(argc) + 2; // *<argc>\r\n
        for (int i = 0; i < argc; ++i) {
            size_t len = argvlen[i];
            total += 1 + numDigits(len) + 2 + len + 2; // $<len>\r\n<data>\r\n
        }
        buf_.reserve(buf_.size() + total);

        // *<argc>\r\n
        buf_ += '*';
        appendNum(argc);
        buf_ += "\r\n";

        // $<len>\r\n<data>\r\n for each argument
        for (int i = 0; i < argc; ++i) {
            buf_ += '$';
            appendNum(argvlen[i]);
            buf_ += "\r\n";
            buf_.append(argv[i], argvlen[i]);
            buf_ += "\r\n";
        }
    }

    [[nodiscard]] const char* data() const noexcept { return buf_.data() + consumed_; }
    [[nodiscard]] size_t size() const noexcept { return buf_.size() - consumed_; }
    [[nodiscard]] bool empty() const noexcept { return size() == 0; }

    void consume(size_t n) noexcept {
        consumed_ += n;
        if (consumed_ > buf_.size() / 2 && consumed_ > 1024) {
            buf_.erase(0, consumed_);
            consumed_ = 0;
        }
    }

    void clear() noexcept {
        buf_.clear();
        consumed_ = 0;
    }

private:
    void appendNum(size_t n) {
        char tmp[20];
        int len = 0;
        if (n == 0) {
            buf_ += '0';
            return;
        }
        while (n > 0) {
            tmp[len++] = '0' + static_cast<char>(n % 10);
            n /= 10;
        }
        for (int i = len - 1; i >= 0; --i)
            buf_ += tmp[i];
    }

    static size_t numDigits(size_t n) noexcept {
        if (n == 0) return 1;
        size_t d = 0;
        while (n > 0) { ++d; n /= 10; }
        return d;
    }

    std::string buf_;
    size_t consumed_ = 0;
};

} // namespace jcailloux::relais::io

#endif // JCX_RELAIS_IO_REDIS_RESP_WRITER_H
