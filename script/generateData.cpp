#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
using namespace std;

// usage: argv[1] is the rows of the matrix, argv[2] is the columns of the matrix
// after build this code, turn the out to the file to store matrix
int main(int argc, char *argv[])
{
	int row, column;
	float sp;
	long long cnt = 0, total;
	sscanf(argv[1], "%d", &row);
	sscanf(argv[2], "%d", &column);
	sscanf(argv[3], "%f", &sp);
	sp = sp >= 0 ? 1.0 - sp : -1;
	total = row * column * sp;
	// printf("%d,%d,%f\n", row, column, sp);
	srand(time(NULL));
	for (int i = 0; i < row; i++)
	{
		// fprintf(stdout,"%d:",i);
		for (int j = 0; j < column; j++)
		{
			float tmp;
			if (sp < 0)
			{
				tmp = ((float)rand()) / RAND_MAX * 1;
				tmp = tmp > 0.5 ? 1 : 0;
			}
			else
			{
				tmp = ((float)rand()) / RAND_MAX * 5;
				if (cnt < total && ((tmp < 5 * sp) || (total - cnt == row * column - (i * column + j))))
				{
					tmp = 0.0;
					cnt++;
				}
			}
			fprintf(stdout, "%f", tmp);
			if (j < column - 1)
				fprintf(stdout, ",");
		}
		fprintf(stdout, "\n");
	}
	return 0;
}
