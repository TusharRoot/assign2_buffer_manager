#include "hash_table.h"
#include <stdio.h>

// main method
void
main (void)
{
    HT_TableHandle ht;
    if (initHashTable(&ht, 20) == 1)
    {
        printf("initHashTable failed\n");
        return;
    }
    printf("initHashTable succeeded\n");
    for (int i = 0; i <= 200; i++)
    {
        if (setValue(&ht, i, i + 1) == 1)
        {
            printf("setValue failed\n");
            return;
        }
    }
    printf("setValue succeeded\n");
    for (int i = 0; i <= 200; i += 5)
    {
        if (removePair(&ht, i) == 1)
        {
            printf("removePair failed\n");
            return;
        }
    }
    for (int i = 0; i <= 200; i++)
    {
        if (i % 5 == 0)
        {
            int v;
            if (getValue(&ht, i, &v) == 0)
            {
                printf("getValue should have returned 1\n");
                return;
            }
        }
        else
        {
            int v;
            if (getValue(&ht, i, &v) == 1 || v != i + 1)
            {
                printf("getValue failed\n");
                return;
            }
        }
    }
    printf("removePair & getValue succeeded\n*** freeing table ***\n");
    freeHashTable(&ht);
}
