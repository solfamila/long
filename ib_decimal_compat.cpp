#include "Decimal.h"

#include <bit>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>

namespace {

double decodeDecimal(Decimal value) {
    if (value == UNSET_DECIMAL) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    return std::bit_cast<double>(value);
}

Decimal encodeDecimal(double value) {
    if (std::isnan(value)) {
        return std::bit_cast<Decimal>(std::numeric_limits<double>::quiet_NaN());
    }

    Decimal bits = std::bit_cast<Decimal>(value);
    if (bits == UNSET_DECIMAL) {
        return std::bit_cast<Decimal>(std::numeric_limits<double>::infinity());
    }
    return bits;
}

void clearFlags(unsigned int* flags) {
    if (flags) {
        *flags = 0;
    }
}

} // namespace

extern "C" Decimal __bid64_add(Decimal lhs, Decimal rhs, unsigned int, unsigned int* flags) {
    clearFlags(flags);
    return encodeDecimal(decodeDecimal(lhs) + decodeDecimal(rhs));
}

extern "C" Decimal __bid64_sub(Decimal lhs, Decimal rhs, unsigned int, unsigned int* flags) {
    clearFlags(flags);
    return encodeDecimal(decodeDecimal(lhs) - decodeDecimal(rhs));
}

extern "C" Decimal __bid64_mul(Decimal lhs, Decimal rhs, unsigned int, unsigned int* flags) {
    clearFlags(flags);
    return encodeDecimal(decodeDecimal(lhs) * decodeDecimal(rhs));
}

extern "C" Decimal __bid64_div(Decimal lhs, Decimal rhs, unsigned int, unsigned int* flags) {
    clearFlags(flags);
    return encodeDecimal(decodeDecimal(lhs) / decodeDecimal(rhs));
}

extern "C" Decimal __bid64_from_string(char* str, unsigned int, unsigned int* flags) {
    clearFlags(flags);

    if (str == nullptr || *str == '\0') {
        return UNSET_DECIMAL;
    }

    char* end = nullptr;
    double parsed = std::strtod(str, &end);
    if (end == str) {
        return UNSET_DECIMAL;
    }
    return encodeDecimal(parsed);
}

extern "C" void __bid64_to_string(char* out, Decimal value, unsigned int* flags) {
    clearFlags(flags);

    if (out == nullptr) {
        return;
    }

    if (value == UNSET_DECIMAL) {
        std::strcpy(out, "+NaN");
        return;
    }

    const double decoded = decodeDecimal(value);
    if (std::isnan(decoded)) {
        std::strcpy(out, "+NaN");
        return;
    }

    if (std::isinf(decoded)) {
        std::strcpy(out, decoded < 0.0 ? "-Inf" : "+Inf");
        return;
    }

    std::snprintf(out, 64, "%.15g", decoded);
}

extern "C" double __bid64_to_binary64(Decimal value, unsigned int, unsigned int* flags) {
    clearFlags(flags);
    return decodeDecimal(value);
}

extern "C" Decimal __binary64_to_bid64(double value, unsigned int, unsigned int* flags) {
    clearFlags(flags);
    return encodeDecimal(value);
}
