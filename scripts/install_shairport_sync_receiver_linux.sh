#!/bin/sh
set -eu

SHAIRPORT_SYNC_VERSION=5.2.1
SHAIRPORT_SYNC_SOURCE_SHA256=8f97d1a6e045bc3765b10d0cd64abe467eba343af89fa1e158f7fa28b73c4ab6
TATER_SHAIRPORT_BUILD_REVISION=1

if [ "$(uname -s)" != "Linux" ]; then
    echo "This installer is for native Linux installs." >&2
    exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
tater_dir=$(CDPATH= cd -- "${script_dir}/.." && pwd)
install_dir="${TATER_RUNTIME_DIR:-${tater_dir}/.runtime}/external_audio/shairport-sync-v${SHAIRPORT_SYNC_VERSION}"
receiver_bin="${install_dir}/bin/shairport-sync"
revision_file="${install_dir}/.tater-build-revision"

receiver_is_ready() {
    [ -x "${receiver_bin}" ] \
        && [ "$(cat "${revision_file}" 2>/dev/null || true)" = "${TATER_SHAIRPORT_BUILD_REVISION}" ] \
        && "${receiver_bin}" -V 2>&1 | grep -q "^${SHAIRPORT_SYNC_VERSION}-" \
        && "${receiver_bin}" -h 2>&1 | grep -q -- "--service-type" \
        && "${receiver_bin}" -h 2>&1 | grep -q -- "stdout"
}

if receiver_is_ready; then
    echo "Shairport Sync ${SHAIRPORT_SYNC_VERSION} is already ready at ${receiver_bin}"
    exit 0
fi

for tool in autoreconf automake libtoolize pkg-config patch make cc curl; do
    if ! command -v "${tool}" >/dev/null 2>&1; then
        echo "Missing Shairport Sync build tool: ${tool}" >&2
        exit 1
    fi
done

build_root=$(mktemp -d "${TMPDIR:-/tmp}/tater-shairport-sync.XXXXXX")
cleanup() {
    rm -rf "${build_root}"
}
trap cleanup EXIT HUP INT TERM

archive="${build_root}/shairport-sync.tar.gz"
source_dir="${build_root}/source"
mkdir -p "${source_dir}" "${install_dir}/bin"

curl -fsSL \
    "https://codeload.github.com/mikebrady/shairport-sync/tar.gz/refs/tags/${SHAIRPORT_SYNC_VERSION}" \
    -o "${archive}"

actual_sha=$(sha256sum "${archive}" | awk '{print $1}')
if [ "${actual_sha}" != "${SHAIRPORT_SYNC_SOURCE_SHA256}" ]; then
    echo "Shairport Sync source checksum mismatch." >&2
    exit 1
fi

tar -xzf "${archive}" -C "${source_dir}" --strip-components=1
patch -d "${source_dir}" -p1 < "${script_dir}/shairport_sync_configured_port.patch"

cd "${source_dir}"
autoreconf -fi
./configure \
    --prefix="${install_dir}" \
    --with-ssl=openssl \
    --with-tinysvcmdns \
    --with-soxr \
    --with-stdout \
    --with-metadata \
    --with-metadata-multicast

rm -f "${receiver_bin}"
make -j "${TATER_SHAIRPORT_BUILD_JOBS:-4}"
make install
printf '%s\n' "${TATER_SHAIRPORT_BUILD_REVISION}" > "${revision_file}"

if ! receiver_is_ready; then
    echo "Shairport Sync built without the required classic/stdout support." >&2
    exit 1
fi

echo "Shairport Sync ${SHAIRPORT_SYNC_VERSION} is ready at ${receiver_bin}"
