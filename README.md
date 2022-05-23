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