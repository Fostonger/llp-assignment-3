#! /bin/bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/
mkdir cmakeFiles
cd cmakeFiles
cmake ..
cp ../writing_script.py ../build/
make
