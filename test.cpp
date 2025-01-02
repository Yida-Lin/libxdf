#include "xdf.h"
#include <iostream>

int main(int argc, char** argv) {
	if(argc < 2) {
		std::cerr << "Usage: " << argv[0] << " filename\n";
		return 1;
	}
	Xdf XDFdata;
	XDFdata.load_xdf(argv[1]);
	for(const auto& stream: XDFdata.streams) {
		std::cout << "Stream: " << stream.info.name
				  << ' ' << stream.info.channel_count << " channels "
				  << stream.info.sample_count << " samples\n";
	}
}
