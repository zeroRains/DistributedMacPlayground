from math import ceil

K = 1000
M = K*K

row = 50*K
col = 50*K
block_size = 1000

rb = ceil(row/block_size)
cb = ceil(col/block_size)

file_name = f"{row}x{col}x{block_size}_matrix_index.csv"

with open(file_name,"w+",encoding="utf-8") as f:
    for i in range(rb):
        for j in range(cb):
            if i==rb-1 and j==cb-1:
                f.write(f"{i+1},{j+1}")
            else:
                f.write(f"{i+1},{j+1}\n")
print(f"finish output {file_name}")