// Turn store sales into binary format
#include<stdio.h>
#include<stdlib.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<sys/fcntl.h>
#include<unistd.h>
#include<string.h>

char buffer[10000];
unsigned long buf_off;
char converted[10000];
unsigned long conv_off;
void convert_ulong()
{
  char token[100];
  unsigned long value = 0;
  int off = 0;
  while(buffer[++buf_off] != '|')
    token[off++] = buffer[buf_off];
  token[off] = 0;
  sscanf(token, "%lu", &value);
  //printf("ULONG %d %s->%lu\n",off, token, value);
  memcpy(converted + conv_off, &value, sizeof(unsigned long));
  conv_off += sizeof(unsigned long);
}

void convert_float()
{
  char token[100];
  float value = 0.0f;
  int off = 0;
  while(buffer[++buf_off] != '|')
    token[off++] = buffer[buf_off];
  token[off] = 0;
  sscanf(token, "%f", &value);
  //printf("FLOAT %d %s->%f\n",off, token, value);
  memcpy(converted + conv_off, &value, sizeof(float));
  conv_off += sizeof(float);
}

int main(int argc, char *argv[])
{
  if(argc != 3) {
    printf("Usage: %s infile outfile\n", argv[0]);
    exit(-1);
  }
  FILE *fd_in = fopen(argv[1], "r");
  if(fd_in == NULL) {
    printf("Failed to open input file\n");
    exit(-1);
  }
  int fd_out = open(argv[2], O_WRONLY|O_CREAT, S_IRWXU);
  if(fd_out == -1) {
    printf("Failed to open output file\n");
    exit(-1);
  }
  while(1) {
    if(fgets(buffer, 10000, fd_in) == NULL)
      break;
    buf_off  = -1;
    conv_off = 0;
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_ulong();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    convert_float();
    
    int bytes_to_write = conv_off;
    char *data = converted;
    while(bytes_to_write) {
      int e = write(fd_out, data, bytes_to_write);
      if(e < 0) {
	printf("write failed\n");
	exit(-1);
      }
      bytes_to_write -= e;
      data += e;
    }
  }
  fclose(fd_in);
  close(fd_out);
}
