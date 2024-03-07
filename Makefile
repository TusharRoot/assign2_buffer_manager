CC := gcc
SRC := dberror.c storage_mgr.c buffer_mgr.c buffer_mgr_stat.c test_assign2_1.c
OBJ := dberror.c storage_mgr.o buffer_mgr.o buffer_mgr_stat.o test_assign2_1.o
assignment2: $(OBJ)
		$(CC) -o test_assign1 $(SRC)
$(OBJ): $(SRC)
		$(CC) -g -c $(SRC)
run: assignment1
		./test_assign1
clean:
		rm -rf test_assign1 *.o