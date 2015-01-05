#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUCKET_COUNT 1024
#define INDEX_COUNT 1024

typedef struct bucket_t {
  char name[128];
  int value;
  struct bucket_t *next;
} bucket_t;

// FIXIT union
typedef struct cmd_t {
  char cmd[16];
  char var[16]; //optional
  int value; //optional
} cmd_t;

typedef struct journal_t {
  cmd_t commands[16][16]; // [a][b] a is transaction, b is commands
} journal_t;

typedef struct dorkdb_t {
  journal_t *journal;
  bucket_t *buckets[BUCKET_COUNT];
  short index[1024];
} dorkdb_t;

// public FIXIT move to .h
dorkdb_t *init_db();
void halt_db(dorkdb_t *db);
void db_set(cmd_t cmd, dorkdb_t *db);
void repl(dorkdb_t *db);

int hash(char *s);

int main(int argc, char **argv) {
  dorkdb_t *db = init_db();
  repl(db);
  halt_db(db);
}

dorkdb_t *init_db() {
  dorkdb_t *db = (dorkdb_t *)malloc(sizeof(dorkdb_t));
  memset(db->buckets, 0, 1024*sizeof(bucket_t));
  memset(db->index, 0, 1024*sizeof(short));
  return db;
}

void repl(dorkdb_t *db) {
}

void db_set(cmd_t cmd, dorkdb_t *db) {
  int idx = hash(cmd.var);
  bucket_t *bucket = (bucket_t *)malloc(sizeof(bucket_t));
  strcpy(bucket->name, cmd.var);
  bucket->value = cmd.value;
  if (db->buckets[idx] == NULL) {
    bucket->next = NULL;
    db->buckets[idx] = bucket;
  }
  else {
    bucket_t *cur = db->buckets[idx];
    while (cur->next != NULL) {
      cur = cur->next;
    }
    cur->next = bucket;
  }
  if (cmd.value < INDEX_COUNT) {
    db->index[cmd.value]++;
  }
  else {
    // FIXIT TODO index overflow, just scan table
  }
}

void db_unset(cmd_t cmd, dorkdb_t *db) {
  int idx = hash(cmd.var);
  int removed = 1;
  bucket_t *bucket = db->buckets[idx];
  bucket_t *prev, *cur = bucket;
  while (cur != NULL) {
    if (strcmp(cur->name, cmd.var) == 0) {
      prev->next = cur->next;
      free(cur);
      cur = NULL;
      removed = 1;
      break;
    }
    prev = cur;
    cur = cur->next;
  }

  if (cmd.value < INDEX_COUNT && removed) {
    db->index[cmd.value]--;
  }
  else {
    // FIXIT TODO index overflow, just scan table
  }
}

int hash(char *s) {
  int sum = 0;
  while (*s) sum += *s;
  return (sum*351779) % BUCKET_COUNT;
}

void halt_db(dorkdb_t *db) {
  if (db->buckets != NULL) {
    // FIXIT free buckets
  }
  if (db->journal != NULL) {
    // FIXIT free journal
    free(db->journal);
  }
  free(db);
  db = NULL;
}

