#!/usr/bin/env bash

if [[ -z $1 ]]; then echo "usage: mobdb.sh <pgbin_dir> [working-dir]"; exit 1; fi
project_dir=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

pgbin_dir="$1"
if [[ ! -f ${pgbin_dir}/pg_config ]]; then echo "${pgbin_dir}/pg_config was not found"; exit 1; fi

mobdb_dir=${2:-"$project_dir/meos-sys/mobdb"}
mobdb_src_dir="${mobdb_dir}/src"
mobdb_build_dir="${mobdb_dir}/build"

mkdir -p "${mobdb_dir}"

if [[ ! -d ${mobdb_src_dir} ]]; then
  git clone --depth=1 --branch v1.1.0 https://github.com/MobilityDB/MobilityDB.git "${mobdb_src_dir}"
fi

if [[ ! -f ${mobdb_dir}/lib/libmeos.a ]]; then
  mkdir -p "${mobdb_build_dir}"
  cd "${mobdb_src_dir}" || exit 1
  sed -i 's/SHARED/STATIC/' meos/CMakeLists.txt
  sed -i 's#/usr/local#${CMAKE_INSTALL_PREFIX}#' meos/CMakeLists.txt

  cd "${mobdb_build_dir}" || exit 1
  cmake -DMEOS=ON \
        -DPOSTGRESQL_BIN="${pgbin_dir}" \
        -DCMAKE_C_COMPILER=/usr/bin/clang \
        -DCMAKE_CXX_COMPILER=/usr/bin/clang++ \
        -DCMAKE_INSTALL_PREFIX="/usr/local" \
        "${mobdb_src_dir}"

  cd "${mobdb_build_dir}"/meos || exit 1
  make
  make install
fi
