test_assign2_1: 
	gcc -o test_assign2_1.o test_assign2_1.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c hash_table.c

test_assign2_2: 
	gcc -o test_assign2_2.o test_assign2_2.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c hash_table.c

test_assign2_3: 
	gcc -o test_assign2_3.o test_assign2_3.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c hash_table.c

test_hash_table: 
	gcc -o test_hash_table.o test_hash_table.c hash_table.c

.PHONY: clean
clean:
	rm -f test_assign2_1.o
	rm -f test_assign2_2.o
	rm -f test_assign2_3.o
	rm -f test_hash_table.o