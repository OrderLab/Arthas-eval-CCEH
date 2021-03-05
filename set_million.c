#include <stdio.h>
#include <stdlib.h>
#include <string.h>
int main(){
	FILE *fp;
	fp = fopen("data.txt", "w+");
	//int cLen = 524*1024*1024;
	//char *str = malloc(cLen + 1);
	//memset(str, 'a', cLen);
	//str[cLen] = 0;
	//fprintf(fp, "%s", str);
	for(int i = 0; i < 1000000; i++){
		//i = rand() % 10000;
		fprintf(fp, "%d\n", i);
	}

}
