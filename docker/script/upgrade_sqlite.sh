# a quick script to upgrade sqlite so that airflow commands would work on the docker container
# this is hacky, optional, and safe to ignore, and I really only need it for hotfixes

sudo yum -y install wget tar gzip gcc make expect

sudo wget https://www.sqlite.org/src/tarball/sqlite.tar.gz
sudo tar xzf sqlite.tar.gz
sudo cd sqlite/
export CFLAGS="-DSQLITE_ENABLE_FTS3 \
    -DSQLITE_ENABLE_FTS3_PARENTHESIS \
    -DSQLITE_ENABLE_FTS4 \
    -DSQLITE_ENABLE_FTS5 \
    -DSQLITE_ENABLE_JSON1 \
    -DSQLITE_ENABLE_LOAD_EXTENSION \
    -DSQLITE_ENABLE_RTREE \
    -DSQLITE_ENABLE_STAT4 \
    -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT \
    -DSQLITE_SOUNDEX \
    -DSQLITE_TEMP_STORE=3 \
    -DSQLITE_USE_URI \
    -O2 \
    -fPIC"
export PREFIX="/usr/local"
sudo LIBS="-lm" ./configure --disable-tcl --enable-shared --enable-tempstore=always --prefix="$PREFIX"
make
sudo make install

export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH