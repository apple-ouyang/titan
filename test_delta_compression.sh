#!/bin/bash
output=~/delta_compress.log
echo test Titan with database Wikipedia
echo test with original gdelta, xdelta, edelta
git checkout original_gdelta

pushd third-party/gdelta
git checkout original_gdelta
popd

mkdir build -p && pushd build
cmake ..
make -j $(nproc)
./titan_delta_compression_test 2>&1 | tee $output
popd
echo test with stream gdelta
git checkout stream_gdelta

pushd third-party/gdelta
git checkout stream_gdelta
popd

mkdir build -p && pushd build
cmake ..
make -j $(nproc)
./titan_delta_compression_test 2>&1 | tee -a $output
popd
echo test done! Log is located in $output