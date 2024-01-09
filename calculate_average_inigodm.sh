#!/bin/sh
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Uncomment below to use sdk
# source "$HOME/.sdkman/bin/sdkman-init.sh"
# sdk use java 21.0.1-graal 1>&2
./mvnw clean verify
JAVA_OPTS="--enable-preview"
time java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_inigodm

#335.22user 8.60system 5:38.16elapsed 101%CPU (0avgtext+0avgdata 299900maxresident)k
#25357664inputs+2280outputs (1major+77916minor)pagefaults 0swaps
#337666 ms

#336.20user 8.21system 5:36.27elapsed 102%CPU (0avgtext+0avgdata 308484maxresident)k
#19962720inputs+2128outputs (1major+83691minor)pagefaults 0swaps
#It take 335844 ms

#994.06user 20.84system 3:08.40elapsed 538%CPU (0avgtext+0avgdata 3816508maxresident)k
#22691008inputs+1216outputs (1major+4101570minor)pagefaults 0swaps
#It take 187710 ms
#980.31user 29.59system 3:26.91elapsed 488%CPU (0avgtext+0avgdata 3869640maxresident)k
#26940840inputs+1440outputs (1major+5556123minor)pagefaults 0swaps

#It take 196349 ms
#1016.19user 25.06system 3:17.06elapsed 528%CPU (0avgtext+0avgdata 3847832maxresident)k
# 24993336inputs+1344outputs (2major+4684439minor)pagefaults 0swaps

#It take 189321 ms
#It's ok? true
#982.51user 23.28system 3:09.74elapsed 530%CPU (0avgtext+0avgdata 3862440maxresident)k
#24687056inputs+1224outputs (0major+4068463minor)pagefaults 0swaps

#It take 189088 ms
#972.76user 23.40system 3:09.53elapsed 525%CPU (0avgtext+0avgdata 3855516maxresident)k
#23103072inputs+1376outputs (0major+4413284minor)pagefaults 0swaps

#It take 186863 ms
#956.10user 22.83system 3:07.30elapsed 522%CPU (0avgtext+0avgdata 3797532maxresident)k
#24689704inputs+1296outputs (0major+4077628minor)pagefaults 0swaps




