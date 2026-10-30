#!/usr/bin/env bash
# =============================================================================
# Build Official Pony Runtime (`libponyrt`) from `ponylang/ponyc`
#
# Usage:
#   ./benchmarks/build_libponyrt.sh
#
# Output:
#   .tmp/ponyc_build/libponyrt.a
#   .tmp/ponyc_build/libponyrt.so
# =============================================================================

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PONY_SRC="${REPO_ROOT}/.tmp/ponyc_src"
PONY_BUILD="${REPO_ROOT}/.tmp/ponyc_build"
PONY_TAG="${PONY_TAG:-0.77.2}"

mkdir -p "${REPO_ROOT}/.tmp"

if [ ! -d "${PONY_SRC}/src/libponyrt" ]; then
    echo "==> Shallow-cloning ponylang/ponyc (tag: ${PONY_TAG})..."
    rm -rf "${PONY_SRC}"
    git clone --depth 1 --branch "${PONY_TAG}" https://github.com/ponylang/ponyc.git "${PONY_SRC}"
fi

mkdir -p "${PONY_BUILD}/obj"
rm -f "${PONY_BUILD}/obj/"*.o "${PONY_BUILD}/libponyrt.a" "${PONY_BUILD}/libponyrt.so"

cat > "${PONY_BUILD}/pony_version.h" <<EOF
#ifndef PONY_VERSION_H
#define PONY_VERSION_H
#define PONY_VERSION "${PONY_TAG}"
#define PONY_VERSION_STR "${PONY_TAG} [release]"
#define PONY_COMPILER "gcc"
#define PONY_ARCH "x86_64"
#define PONY_BUILD_CONFIG "release"
#endif
EOF

echo "==> Compiling libponyrt (${PONY_TAG}) with gcc -O3 -march=native..."
find "${PONY_SRC}/src/libponyrt" -name "*.c" ! -path "*/platform/windows/*" | while read -r cfile; do
    rel="${cfile#${PONY_SRC}/src/libponyrt/}"
    obj="${PONY_BUILD}/obj/${rel//\//_}.o"
    gcc -O3 -march=native -fPIC -std=gnu11 -pthread \
        -DPONY_NDEBUG -DPOOL_USE_DEFAULT -DPONY_BUILD_CONFIG=\"release\" \
        -I"${PONY_BUILD}" \
        -I"${PONY_SRC}/src/libponyrt" \
        -I"${PONY_SRC}/src/common" \
        -c "${cfile}" -o "${obj}"
done

ar rcs "${PONY_BUILD}/libponyrt.a" "${PONY_BUILD}/obj/"*.o
gcc -shared -o "${PONY_BUILD}/libponyrt.so" "${PONY_BUILD}/obj/"*.o -lpthread -latomic -lm -ldl

echo "==> Built:"
ls -lh "${PONY_BUILD}/libponyrt.a" "${PONY_BUILD}/libponyrt.so"
