#include "sqlite3.h"
#include <iostream>
using namespace std;
int main()
{
    sqlite3* db;
    int res = sqlite3_open("databaseName.db", &db);
    if(res)
        //database failed to open
        cout << "Database failed to open" << endl;
    else
    {
        //your database code here
    }
    sqlite3_close(db);
}