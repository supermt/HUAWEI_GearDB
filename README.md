# Update List

# May 21 th

> Change the format of SSTs by changing the builder and reader

- Builder, change it 4k to 8k
    - Will be implemented in the option files in the future
- Add the **placeholder** string in the
    - A series of 0x0c
    - Length is determined by the spare length in the block
- Extend the Header Block
    - Change it from 16 Bytes to 64 Bytes
    - For now, the format is:
        - block id
        - entry count in current block
        - value buffer length
        - key buffer length
        - placeholder length

# May 23 th

> Change the direction of key array to "World hello" style by reversing two times

- Fix compile problems in the CMakeList.txt
- Fix the problem while reading

# May 24 th

> Add the Benchmark tool into the gear_bench

- Fix most of the problems happens when adding the entire Benchmark class into the gear bench
- It can now enable the tuner and some other things.
- Add the function framework into the system.

# May 25 th

> Add the execution function for the three test benchmark

- Validate

# Jun 18 th

> Add the interfaces for Miss SUN Xuan to connect the FPGA processing

# Jun 22 th

> Remove the interfaces to use conventional compaction
> Add the L2 stall