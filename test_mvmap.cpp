#include "mvmap.h"
#include <iostream>

typedef mongo::multiversion_map<std::string, int> mvmap_type;

void dump_map(mvmap_type &mvmap) {
  for (auto i = mvmap.begin(); i != mvmap.end(); i++) {
    std::cout << (*i).first << " -> " << (*i).second << ", ";
  }
  std::cout << std::endl;
}

int main() {
  mvmap_type mvmap;

  mvmap["a"] = 1;
  mvmap["b"] = 2;
  mvmap["c"] = 3;

  dump_map(mvmap);

  mvmap.set_timestamp(3, 4);
  mvmap["a"] = 4;
  mvmap.set_timestamp(4, 5);
  mvmap["b"] = 5;
  mvmap.set_timestamp(5, 6);
  mvmap["c"] = 6;

  dump_map(mvmap);

  mvmap.set_timestamp(6, 7);
  mvmap["a"] = 7;
  mvmap.set_timestamp(6, 8);
  mvmap["b"] = 8;
  mvmap.set_timestamp(6, 9);
  mvmap["c"] = 9;

  dump_map(mvmap);

  mvmap.set_timestamp(6, 6);

  dump_map(mvmap);

  return (0);
}
