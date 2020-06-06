#!/usr/bin/env bash
#rsync -rvz target root@demo2:~
rsync -rvz --progress target jcloud-stream:~
rsync -rvz --progress target jcloud-kafka1:~
#rsync -rvz --progress target compute03:~
#rsync -rvz --progress target compute04:~
#rsync -rvz --progress target lenovo:~