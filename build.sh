#! /usr/bin/env bash

function usage() {
    echo "Usage:"
	echo "    $0 -h                      Display this help message."
	echo "    $0 [options]"
    echo "Options:"
    echo "     -p publish image"
	exit 1;
}

function echo_c {
    printf "\033[0;$1m$2\033[0m\n"
}

function log_info {
    echo_c 33 "$1"
}

function log_success {
    echo_c 32 "$1"
}

function log_err {
    echo_c 35 "$1"
}

PUBLISH=0

while getopts ":hp" opt; do
    case ${opt} in
        h )
			usage
            ;;
        p )
            PUBLISH=1
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
    esac
done

MIRROR="harbor.tianhecloud.com:54616"
IMG_ID="videown/cmps:latest"

if [ -n "$MIRROR" ]; then
    IMG_ID="$MIRROR/$IMG_ID"
fi

log_info "building $IMG_ID"

#DOCKER_BUILDKIT=1 --progress=plain
docker build -t $IMG_ID --build-arg go_proxy=https://goproxy.cn,direct --build-arg https_proxy=http://172.16.2.89:7890 --build-arg http_proxy=http://172.16.2.89:7890 . 

if [ $? -eq "0" ]; then
    log_info "Done building cmps image, tag: $IMG_ID"
else
    log_err "Failed on building cmps."
    exit 1
fi

log_info "Build success"
if [ "$PUBLISH" -eq "1" ]; then
    log_info "Publishing image to $IMG_ID"
    docker push $IMG_ID
fi
