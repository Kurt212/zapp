# Performance Test

The tests were run on a new VPS rented in [Yandex Cloud](https://cloud.yandex.com). 
The setup was:
- Ubuntu 22.04
- 8 CPUs
- 16 GB RAM and 8 GB in the third test
- Fast SSD. Max IOPS 75000 for read and for write. Max bandwidth 1024 MB/s for read and for write
- GO 1.21.4

## First Test. Data fits In RAM and Access pattern is not the worst

The first test shows how performance changes depending on how many segments and threads Zapp uses to serve requests.

The dataset is pretty small. It's 10 bytes for a key, 8 bytes for a value. 10 million pairs is about 200 MB.

WAL is disabled.

Test does the following:
1. Sets all keys
2. Overwrites all keys with new value
3. Calls GET on all keys 10x times
4. Deletes all keys

The values in the table for set, reset, get and delete columns represent the number of QPS.

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 4 | 10 000 000,00 | 0 | 373 933,00 | 331 592,00 | 2 663 528,00 | 437 295,00 |
| 4 | 4 | 10 000 000,00 | 0 | 499 962,00 | 435 116,00 | 2 519 215,00 | 611 444,00 |
| 8 | 4 | 10 000 000,00 | 0 | 637 324,00 | 552 292,00 | 2 661 128,00 | 816 796,00 |
| 16 | 4 | 10 000 000,00 | 0 | 756 311,00 | 682 306,00 | 2 674 709,00 | 1 026 732,00 |
| 32 | 4 | 10 000 000,00 | 0 | 859 994,00 | 791 090,00 | 2 775 531,00 | 1 056 532,00 |
| 64 | 4 | 10 000 000,00 | 0 | 956 093,00 | 899 759,00 | 2 761 275,00 | 1 209 120,00 |
| 128 | 4 | 10 000 000,00 | 0 | 1 014 800,00 | 972 107,00 | 2 737 289,00 | 1 340 862,00 |
| 256 | 4 | 10 000 000,00 | 0 | 1 015 365,00 | 1 014 079,00 | 2 778 790,00 | 1 421 190,00 |
| 512 | 4 | 10 000 000,00 | 0 | 997 623,00 | 976 908,00 | 2 573 473,00 | 1 276 116,00 |

So, as you can see, adding more segments affects writing in a better way. The best performance was achieved with 256 segments.

Then test how performance changes, when Zapp uses all 8 CPUs to serve requests.

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 8 | 10 000 000,00 | 0 | 405 067,00 | 352 943,00 | 3 921 002,00 | 471 348,00 |
| 4 | 8 | 10 000 000,00 | 0 | 590 628,00 | 522 329,00 | 4 082 305,00 | 716 216,00 |
| 8 | 8 | 10 000 000,00 | 0 | 821 137,00 | 764 456,00 | 4 291 660,00 | 1 033 571,00 |
| 16 | 8 | 10 000 000,00 | 0 | 1 045 813,00 | 968 161,00 | 4 231 301,00 | 1 271 658,00 |
| 32 | 8 | 10 000 000,00 | 0 | 1 180 611,00 | 1 138 245,00 | 4 332 081,00 | 1 693 473,00 |
| 64 | 8 | 10 000 000,00 | 0 | 1 338 284,00 | 1 296 640,00 | 4 418 378,00 | 1 874 350,00 |
| 128 | 8 | 10 000 000,00 | 0 | 1 415 410,00 | 1 365 690,00 | 4 364 593,00 | 1 940 328,00 |
| 256 | 8 | 10 000 000,00 | 0 | 1 346 173,00 | 1 396 212,00 | 4 208 976,00 | 1 937 655,00 |
| 512 | 8 | 10 000 000,00 | 0 | 1 424 155,00 | 1 350 444,00 | 3 357 236,00 | 2 032 624,00 |

As you can see, adding more CPUs affects performance in a good way. Again, the best performance was achieved with 256 segments. And it's better, that 4 CPUs.

## Second test. WAL enabled

Test does pretty much the same thing. But WAL feature is enabled.

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2048 | 4096 | 10 000 000,00 | 1 | 38 857,00 | 38 149,00 | 3 922 993,00 | 34 895,00 |
| 1024 | 4096 | 10 000 000,00 | 1 | 36 002,00 | 36 121,00 | 4 432 106,00 | 37 773,00 |
| 512 | 4096 | 10 000 000,00 | 1 | 38 665,00 | 38 803,00 | 4 443 870,00 | 37 731,00 |
| 256 | 4096 | 10 000 000,00 | 1 | 34 419,00 | 38 120,00 | 4 677 546,00 | 38 470,00 |
| 128 | 4096 | 10 000 000,00 | 1 | 29 892,00 | 26 591,00 | 4 489 420,00 | 29 852,00 |

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2048 | 2048 | 10 000 000,00 | 1 | 37 606,00 | 37 536,00 | 4 447 245,00 | 38 744,00 |
| 1024 | 2048 | 10 000 000,00 | 1 | 36 281,00 | 27 504,00 | 4 384 646,00 | 31 113,00 |
| 512 | 2048 | 10 000 000,00 | 1 | 37 590,00 | 37 482,00 | 4 459 243,00 | 37 210,00 |
| 256 | 2048 | 10 000 000,00 | 1 | 37 864,00 | 37 620,00 | 4 551 005,00 | 38 770,00 |
| 128 | 2048 | 10 000 000,00 | 1 | 29 584,00 | 30 213,00 | 4 647 075,00 | 24 068,00 |

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2048 | 1024 | 10 000 000,00 | 1 | 32 326,00 | 35 830,00 | 4 502 783,00 | 36 998,00 |
| 1024 | 1024 | 10 000 000,00 | 1 | 35 390,00 | 34 330,00 | 4 414 394,00 | 36 462,00 |
| 512 | 1024 | 10 000 000,00 | 1 | 37 086,00 | 37 252,00 | 4 563 046,00 | 38 311,00 |
| 256 | 1024 | 10 000 000,00 | 1 | 36 102,00 | 36 874,00 | 4 481 993,00 | 37 102,00 |
| 128 | 1024 | 10 000 000,00 | 1 | 29 159,00 | 28 529,00 | 4 485 929,00 | 28 288,00 |

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 512 | 512 | 10 000 000,00 | 1 | 35 048,00 | 35 495,00 | 4 405 326,00 | 35 941,00 |
| 256 | 512 | 10 000 000,00 | 1 | 34 715,00 | 34 739,00 | 4 387 241,00 | 34 898,00 |
| 128 | 512 | 10 000 000,00 | 1 | 28 127,00 | 26 494,00 | 4 445 724,00 | 29 319,00 |

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1024 | 256 | 10 000 000,00 | 1 | 32 926,00 | 32 673,00 | 3 799 703,00 | 33 425,00 |
| 512 | 256 | 10 000 000,00 | 1 | 31 989,00 | 31 889,00 | 4 440 934,00 | 32 090,00 |
| 256 | 256 | 10 000 000,00 | 1 | 24 099,00 | 31 793,00 | 4 555 855,00 | 31 206,00 |
| 128 | 256 | 10 000 000,00 | 1 | 25 760,00 | 22 848,00 | 4 622 398,00 | 25 598,00 |

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1024 | 128 | 10 000 000,00 | 1 | 25 581,00 | 25 605,00 | 4 397 476,00 | 26 150,00 |
| 512 | 128 | 10 000 000,00 | 1 | 21 585,00 | 22 336,00 | 4 362 231,00 | 24 773,00 |
| 256 | 128 | 10 000 000,00 | 1 | 25 386,00 | 25 968,00 | 4 584 681,00 | 25 475,00 |
| 128 | 128 | 10 000 000,00 | 1 | 23 479,00 | 23 536,00 | 4 520 420,00 | 20 908,00 |

| segments | threads | keys | wal | set | reset | get | delete |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 512 | 64 | 10 000 000,00 | 1 | 18 757,00 | 16 827,00 | 4 442 273,00 | 18 261,00 |
| 256 | 64 | 10 000 000,00 | 1 | 17 324,00 | 19 478,00 | 4 585 277,00 | 19 484,00 |
| 128 | 64 | 10 000 000,00 | 1 | 18 993,00 | 19 076,00 | 4 618 546,00 | 18 692,00 |
| 64 | 64 | 10 000 000,00 | 1 | 15 471,00 | 12 084,00 | 4 593 984,00 | 15 997,00 |

As you can see, the more threads Zapp uses, the better write performance is. And read performance doesn't change at all and is always about 4-4.5 million QPS.

## Third test. Change dataset size and access pattern

So this test was supposed to show how Zapp would perform in a real life situation. The changes are:
- VPS has now only 8 GM RAM.
- The dataset changes dramatically, so it doesn't fit in RAM anymore.
- The keys are shuffled randomly before each operation: set, reset, get, delete. This makes sure, that neighbor keys are not served from OS cache. And we would have a lot of cache misses.
- Key size is still 10 bytes. But value size is now variable.

WAL disabled:

| segments | threads | keys | value_size (bytes) | wal | set | reset | get | delete | dataset (GBs) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32,00 | 4,00 | 10 000 000,00 | 8,00 | 0,00 | 887 224,00 | 745 880,00 | 4 173 253,00 | 1 068 344,00 | 0,07 |
| 32,00 | 4,00 | 10 000 000,00 | 1 024,00 | 0,00 | 265 889,00 | 9 110,00 | 1 288 999,00 | 9 467,00 | 9,54 |
| 128,00 | 4,00 | 10 000 000,00 | 8,00 | 0,00 | 1 093 336,00 | 903 243,00 | 4 226 864,00 | 1 269 663,00 | 0,07 |
| 128,00 | 4,00 | 10 000 000,00 | 1 024,00 | 0,00 | 267 764,00 | 8 896,00 | 1 304 659,00 | 9 541,00 | 9,54 |
| 256,00 | 4,00 | 10 000 000,00 | 8,00 | 0,00 | 1 065 452,00 | 931 020,00 | 4 139 351,00 | 1 330 087,00 | 0,07 |
| 256,00 | 4,00 | 10 000 000,00 | 1 024,00 | 0,00 | 288 222,00 | 9 627,00 | 1 283 536,00 | 9 920,00 | 9,54 |
| 256,00 | 4,00 | 10 000 000,00 | 4 096,00 | 0,00 | 99 532,00 | 6 967,00 | 466 266,00 | 7 122,00 | 38,15 |

| segments | threads | keys | value_size (bytes) | wal | set | reset | get | delete | dataset (GBs) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32,00 | 8,00 | 10 000 000,00 | 1 024,00 | 0,00 | 294 777,00 | 13 104,00 | 1 352 478,00 | 12 327,00 | 9,54 |
| 128,00 | 8,00 | 10 000 000,00 | 1 024,00 | 0,00 | 320 562,00 | 15 360,00 | 1 312 204,00 | 15 043,00 | 9,54 |
| 256,00 | 8,00 | 10 000 000,00 | 1 024,00 | 0,00 | 337 155,00 | 14 425,00 | 1 296 814,00 | 14 613,00 | 9,54 |
| 256,00 | 8,00 | 10 000 000,00 | 4 096,00 | 0,00 | 100 960,00 | 10 397,00 | 448 865,00 | 11 160,00 | 38,15 |

And with WAL enabled:

| segments | threads | keys | value_size (bytes) | wal | set | reset | get | delete | dataset (GBs) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2 048,00 | 2 048,00 | 10 000 000,00 | 8,00 | 1,00 | 38 626,00 | 38 459,00 | 5 438 239,00 | 39 895,00 | 0,07 |
| 2 048,00 | 2 048,00 | 10 000 000,00 | 512,00 | 1,00 | 27 266,00 | 16 757,00 | 2 085 697,00 | 18 024,00 | 4,77 |
| 2 048,00 | 2 048,00 | 10 000 000,00 | 1 024,00 | 1,00 | 24 228,00 | 15 170,00 | 1 343 137,00 | 17 462,00 | 9,54 |
| 2 048,00 | 2 048,00 | 10 000 000,00 | 2 048,00 | 1,00 | 18 901,00 | 12 717,00 | 768 392,00 | 16 875,00 | 19,07 |
| 2 048,00 | 2 048,00 | 10 000 000,00 | 4 096,00 | 1,00 | 13 356,00 | 10 091,00 | 445 141,00 | 16 124,00 | 38,15 |

So, as you can see, the performance of all operations drops terribly, while dataset size grows. WAL feature still makes all requests perform worse. 

And the most important thing is, that, when dataset doesn't fit in RAM and the access pattern is pretty random, the performance is expected to be 10x times worse.
