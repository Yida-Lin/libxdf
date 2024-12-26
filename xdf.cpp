//libxdf is a static C++ library to load XDF files
//Copyright (C) 2017  Yida Lin

//This program is free software: you can redistribute it and/or modify
//it under the terms of the GNU General Public License as published by
//the Free Software Foundation, either version 3 of the License, or
//(at your option) any later version.

//This program is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//GNU General Public License for more details.

//You should have received a copy of the GNU General Public License
//along with this program.  If not, see <http: /www.gnu.org/licenses />.
//If you have questions, contact author at yida.lin@outlook.com


#include "xdf.h"

#include <iostream>
#include <fstream>
#include "pugixml.hpp"  //pugi XML parser
#include <sstream>
#include <algorithm>
#include "smarc/smarc.h"      //resampling library
#include <time.h>       /* clock_t, clock, CLOCKS_PER_SEC */
#include <numeric>      //std::accumulate
#include <functional>   // bind2nd
#include <cmath>
#include <variant>

namespace xdf {
namespace {

Stream MakeStream(const pugi::xml_node doc, const std::string_view stream_header) {
    Stream stream;
    stream.streamHeader = stream_header;
    const pugi::xml_node info = doc.child("info");
    const pugi::xml_node desc = info.child("desc");
    const std::string channel_format = info.child("channel_format").text().get();
    stream.info.channel_count = info.child("channel_count").text().as_int();

    if (channel_format.compare("string") == 0) {
        stream.time_series = std::vector<std::vector<std::string>>(stream.info.channel_count);
    } else if (channel_format.compare("float32") == 0) {
        stream.time_series = std::vector<std::vector<float>>(stream.info.channel_count);
    } else if (channel_format.compare("double64") == 0) {
        stream.time_series = std::vector<std::vector<double>>(stream.info.channel_count);
    } else if (channel_format.compare("int8_t") == 0 ||
               channel_format.compare("int16_t") == 0 ||
               channel_format.compare("int32_t") == 0) {
        stream.time_series = std::vector<std::vector<int>>(stream.info.channel_count);
    } else if (channel_format.compare("int64_t") == 0) {
        stream.time_series = std::vector<std::vector<int64_t>>(stream.info.channel_count);
    }

    stream.info.nominal_srate = info.child("nominal_srate").text().as_double();
    stream.info.name = info.child("name").text().get();
    stream.info.type = info.child("type").text().get();
    stream.info.channel_format = info.child("channel_format").text().get();

    for (auto channel = desc.child("channels").child("channel"); channel; channel = channel.
         next_sibling("channel"))
    {
        stream.info.channels.emplace_back();

        for (auto const& entry : channel.children()) {
            stream.info.channels.back().emplace(entry.name(), entry.child_value());
        }
    }
    stream.sampling_interval = stream.info.nominal_srate > 0
        ? 1 / stream.info.nominal_srate
        : 0;
    return stream;
}


} // namespace

Xdf::Xdf()
{
}

int Xdf::load_xdf(std::string filename)
{
    clock_t time;
    time = clock();


    /*	//uncompress if necessary
     char ext[_MAX_EXT]; //for file extension

     _splitpath_s ( argv[1], NULL, NULL, NULL, NULL, NULL, NULL, ext, NULL );
     if (strcmp(ext, ".xdfz") == 0)
     {
     //uncompress
     }
     */

//    std::vector<int> idmap; //remaps stream id's onto indices in streams


    //===================================================================
    //========================= parse the file ==========================
    //===================================================================


    std::ifstream file(filename, std::ios::in | std::ios::binary);

    if (file.is_open())
    {
        //read [MagicCode]
        std::string magicNumber;
        for (char c; file >> c;)
        {
            magicNumber.push_back(c);
            if (magicNumber.size() == 4)
                break;
        }

        if (magicNumber.compare("XDF:"))
        {
            std::cout << "This is not a valid XDF file.('" << filename << "')\n";
            return -1;
        }

        //for each chunk
        while (1)
        {
            uint64_t ChLen = readLength(file); //chunk length

            if (ChLen == 0)
                break;

            uint16_t tag; //read tag of the chunk, 6 possibilities
            readBin(file, &tag);

            switch (tag)
            {
            case 1: //[FileHeader]
                {
                    char* buffer = new char[ChLen - 2];
                    file.read(buffer, ChLen - 2);
                    fileHeader = buffer;

                    pugi::xml_document doc;

                    doc.load_buffer_inplace(buffer, ChLen - 2);

                    pugi::xml_node info = doc.child("info");

                    version = info.child("version").text().as_float();

                    delete[] buffer;
                }
                break;
            case 2: //read [StreamHeader] chunk
                {
                    //read [StreamID]
                    uint32_t stream_id;
                    Xdf::readBin(file, &stream_id);

                    pugi::xml_document doc;                    

                    //read [Content]
                    char* buffer = new char[ChLen - 6];
                    file.read(buffer, ChLen - 6);
                    doc.load_buffer_inplace(buffer, ChLen - 6);
                    streams[stream_id] = MakeStream(doc, buffer);
                    delete[] buffer;
                }
                break;
            case 3: //read [Samples] chunk
                {
                    //read [stream_id]
                    uint32_t stream_id;
                    Xdf::readBin(file, &stream_id);

                    //read [NumSampleBytes], [NumSamples]
                    uint64_t numSamp = readLength(file);
                    Stream& stream = streams[stream_id];

                    //for each sample
                    for (size_t i = 0; i < numSamp; i++)
                    {
                        //read or deduce time stamp
                        auto tsBytes = readBin<uint8_t>(file);

                        double ts; //temporary time stamp

                        if (tsBytes == 8)
                        {
                            Xdf::readBin(file, &ts);
                            stream.time_stamps.emplace_back(ts);
                        }
                        else
                        {
                            ts = stream.last_timestamp + stream.sampling_interval;
                            stream.time_stamps.emplace_back(ts);
                        }

                        stream.last_timestamp = ts;

                        if (stream.info.channel_format.compare("string") == 0)
                        {                            
                            for (int v = 0; v < stream.info.channel_count; ++v)
                            {
                                auto length = Xdf::readLength(file);
                                char* buffer = new char[length + 1];
                                file.read(buffer, length);
                                buffer[length] = '\0';

                                std::vector<std::vector<std::string>>& time_series = std::get<std::vector<std::vector<std::string>>>(stream.time_series);
                                time_series.emplace_back(buffer);
                            }
                        }
                        else
                        {
                            //read the data
                            if (stream.info.channel_format.compare("float32") == 0)
                            {
                                for (int v = 0; v < stream.info.channel_count; ++v)
                                {
                                    float data;
                                    Xdf::readBin(file, &data);
                                    stream.time_series[v].emplace_back(data);
                                }
                            }
                            else if (streams[index].info.channel_format.compare("double64") == 0)
                            {
                                for (int v = 0; v < streams[index].info.channel_count; ++v)
                                {
                                    double data;
                                    Xdf::readBin(file, &data);
                                    streams[index].time_series[v].emplace_back(data);
                                }
                            }
                            else if (streams[index].info.channel_format.compare("int8_t") == 0)
                            {
                                for (int v = 0; v < streams[index].info.channel_count; ++v)
                                {
                                    int8_t data;
                                    Xdf::readBin(file, &data);
                                    streams[index].time_series[v].emplace_back(data);
                                }
                            }
                            else if (streams[index].info.channel_format.compare("int16_t") == 0)
                            {
                                for (int v = 0; v < streams[index].info.channel_count; ++v)
                                {
                                    int16_t data;
                                    Xdf::readBin(file, &data);
                                    streams[index].time_series[v].emplace_back(data);
                                }
                            }
                            else if (streams[index].info.channel_format.compare("int32_t") == 0)
                            {
                                for (int v = 0; v < streams[index].info.channel_count; ++v)
                                {
                                    int data;
                                    Xdf::readBin(file, &data);
                                    streams[index].time_series[v].emplace_back(data);
                                }
                            }
                            else if (streams[index].info.channel_format.compare("int64_t") == 0)
                            {
                                for (int v = 0; v < streams[index].info.channel_count; ++v)
                                {
                                    int64_t data;
                                    Xdf::readBin(file, &data);
                                    streams[index].time_series[v].emplace_back(data);
                                }
                            }
                        }
                    }
                }
                break;
            case 4: //read [ClockOffset] chunk
                {
                    uint32_t stream_id;
                    Xdf::readBin(file, &stream_id);

                    double collectionTime;
                    double offsetValue;

                    Xdf::readBin(file, &collectionTime);
                    Xdf::readBin(file, &offsetValue);

                    streams[stream_id].clock_times.emplace_back(collectionTime);
                    streams[stream_id].clock_values.emplace_back(offsetValue);
                }
                break;
            case 6: //read [StreamFooter] chunk
                {
                    pugi::xml_document doc;

                    //read [StreamID]
                    uint32_t stream_id;
                    Xdf::readBin(file, &stream_id);

                    char* buffer = new char[ChLen - 6];
                    file.read(buffer, ChLen - 6);
                    streams[stream_id].streamFooter = buffer;

                    doc.load_buffer_inplace(buffer, ChLen - 6);

                    pugi::xml_node info = doc.child("info");

                    streams[stream_id].info.first_timestamp = info.child("first_timestamp").text().as_double();
                    streams[stream_id].info.last_timestamp = info.child("last_timestamp").text().as_double();
                    streams[stream_id].info.measured_srate = info.child("measured_srate").text().as_double();
                    streams[stream_id].info.sample_count = info.child("sample_count").text().as_int();
                    delete[] buffer;
                }
                break;
            case 5: //skip other chunk types (Boundary, ...)
                file.seekg(ChLen - 2, file.cur);
                break;
            default:
                std::cout << "Unknown chunk encountered.\n";
                break;
            }
        }


        //calculate how much time it takes to read the data
        clock_t halfWay = clock() - time;

        std::cout << "it took " << halfWay << " clicks (" << ((float)halfWay) / CLOCKS_PER_SEC << " seconds)"
            << " reading XDF data" << std::endl;


        //==========================================================
        //=============find the min and max time stamps=============
        //==========================================================

        syncTimeStamps();

        findMinMax();

        findMajSR();

        getHighestSampleRate();

        loadSampleRateMap();

        calcTotalChannel();

        loadDictionary();

        calcEffectiveSrate();

        //loading finishes, close file
        file.close();
    }
    else
    {
        std::cout << "Unable to open file" << std::endl;
        return 1;
    }

    return 0;
}

void Xdf::syncTimeStamps()
{
    // Sync time stamps
    for (auto& [id, stream] : this->streams)
    {
        if (!stream.clock_times.empty())
        {
            size_t m = 0; // index iterating through stream.time_stamps
            size_t n = 0; // index iterating through stream.clock_times

            while (m < stream.time_stamps.size())
            {
                if (stream.clock_times[n] < stream.time_stamps[m])
                {
                    while (n < stream.clock_times.size() - 1 && stream.clock_times[n + 1] < stream.time_stamps[m])
                    {
                        n++;
                    }
                    stream.time_stamps[m] += stream.clock_values[n];
                }
                else if (n == 0)
                {
                    stream.time_stamps[m] += stream.clock_values[n];
                }
                m++;
            }
        }
    }

    // Sync event time stamps
    for (auto& elem : this->eventMap)
    {
        if (!this->streams[elem.second].clock_times.empty())
        {
            size_t k = 0; // index iterating through streams[elem.second].clock_times

            while (k < this->streams[elem.second].clock_times.size() - 1)
            {
                if (this->streams[elem.second].clock_times[k + 1] < elem.first.second)
                {
                    k++;
                }
                else
                {
                    break;
                }
            }

            elem.first.second += this->streams[elem.second].clock_values[k];
            // apply the last offset value to the timestamp; if there hasn't yet been an offset value take the first recorded one
        }
    }

    // Update first and last time stamps in stream footer
    for (size_t k = 0; k < this->streams.size(); k++)
    {
        if (streams[k].info.channel_format.compare("string") == 0)
        {
            double min = NAN;
            double max = NAN;

            for (auto const& elem : this->eventMap)
            {
                if (elem.second == (int)k)
                {
                    if (std::isnan(min) || elem.first.second < min)
                    {
                        min = elem.first.second;
                    }

                    if (std::isnan(max) || elem.first.second > max)
                    {
                        max = elem.first.second;
                    }
                }
            }

            streams[k].info.first_timestamp = min;
            streams[k].info.last_timestamp = max;
        }
        else
        {
            if (streams[k].time_stamps.size() > 0)
            {
                streams[k].info.first_timestamp = streams[k].time_stamps.front();
                streams[k].info.last_timestamp = streams[k].time_stamps.back();
            }
        }
    }
}

void Xdf::resample(int userSrate)
{
    //if user entered a preferred sample rate, we resample all the channels to that sample rate
    //Otherwise, we resample all channels to the sample rate that has the most channels

    clock_t time = clock();

#define BUF_SIZE 8192
    for (auto& [id, stream] : streams)
    {
        if (stream.time_series.index() != std::variant_npos &&
            !stream.info.channel_format.compare("string") &&
            stream.info.nominal_srate != userSrate &&
            stream.info.nominal_srate != 0)
        {
            int fsin = stream.info.nominal_srate; // input samplerate
            int fsout = userSrate; // output samplerate
            double bandwidth = 0.95; // bandwidth
            double rp = 0.1; // passband ripple factor
            double rs = 140; // stopband attenuation
            double tol = 0.000001; // tolerance

            // initialize smarc filter
            struct PFilter* pfilt = smarc_init_pfilter(fsin, fsout, bandwidth, rp,
                                                       rs, tol, NULL, 0);
            if (pfilt == NULL)
                continue;

            // initialize smarc filter state
            struct PState* pstate = smarc_init_pstate(pfilt);

            for (auto& row : stream.time_series)
            {
                // initialize buffers
                int read = 0;
                int written = 0;
                const int OUT_BUF_SIZE = (int)smarc_get_output_buffer_size(pfilt, row.size());
                double* inbuf = new double[row.size()];
                double* outbuf = new double[OUT_BUF_SIZE];

                // Fill inbuf with the numeric values from the row
                for (auto& val : row)
                {
                    std::visit([&inbuf, &read](auto&& arg)
                    {
                        using T = std::decay_t<decltype(arg)>;
                        if constexpr (std::is_arithmetic_v<T>)
                        {
                            inbuf[read++] = static_cast<double>(arg); // Convert to double
                        }
                    }, val);
                }
                // resample signal block
                written = smarc_resample(pfilt, pstate, inbuf, read, outbuf, OUT_BUF_SIZE);

                // Replace original values with the resampled output
                read = 0;
                for (auto& val : row)
                {
                    // Only replace numeric values
                    std::visit([&outbuf, &read](auto&& arg)
                    {
                        using T = std::decay_t<decltype(arg)>;
                        if constexpr (std::is_arithmetic_v<T>)
                        {
                            arg = static_cast<T>(outbuf[read++]);
                        }
                    }, val);
                }

                // flushing last values
                written = smarc_resample_flush(pfilt, pstate, outbuf, OUT_BUF_SIZE);

                // Add any remaining flushed values
                read = 0;
                for (auto& val : row)
                {
                    // Only replace numeric values
                    std::visit([&outbuf, &read](auto&& arg)
                    {
                        using T = std::decay_t<decltype(arg)>;
                        if constexpr (std::is_arithmetic_v<T>)
                        {
                            arg = static_cast<T>(outbuf[read++]);
                        }
                    }, val);
                }

                // you are done with converting your signal.
                // If you want to reuse the same converter to process another signal
                // just reset the state:
                smarc_reset_pstate(pstate, pfilt);

                delete[] inbuf;
                delete[] outbuf;
            }
            // release smarc filter state
            smarc_destroy_pstate(pstate);

            // release smarc filter
            smarc_destroy_pfilter(pfilt);
        }
    }
    //resampling finishes here


    //======================================================================
    //===========Calculating total length & total channel count=============
    //======================================================================


    calcTotalLength(userSrate);

    adjustTotalLength();

    time = clock() - time;

    std::cout << "it took " << time << " clicks (" << ((float)time) / CLOCKS_PER_SEC << " seconds)"
        << " resampling" << std::endl;
}

//function of reading the length of each chunk
uint64_t Xdf::readLength(std::ifstream& file)
{
    uint8_t bytes = 0;
    Xdf::readBin(file, &bytes);
    uint64_t length = 0;

    switch (bytes)
    {
    case 1:
        length = readBin<uint8_t>(file);
        break;
    case 4:
        length = readBin<uint32_t>(file);
        break;
    case 8:
        length = readBin<uint64_t>(file);
        break;
    default:
        std::cout << "Invalid variable-length integer length ("
            << static_cast<int>(bytes) << ") encountered.\n";
        return 0;
    }

    return length;
}

void Xdf::findMinMax()
{
    //find the smallest timestamp of all streams
    for (auto const& [id, stream] : streams)
    {
        if (!std::isnan(stream.info.first_timestamp))
        {
            minTS = stream.info.first_timestamp;
            break;
        }
    }
    for (auto const& [id, stream] : streams)
    {
        if (!std::isnan(stream.info.first_timestamp) && stream.info.first_timestamp < minTS)
            minTS = stream.info.first_timestamp;
    }

    //find the max timestamp of all streams
    for (auto const& [id, stream] : streams)
    {
        if (!std::isnan(stream.info.last_timestamp) && stream.info.last_timestamp > maxTS)
            maxTS = stream.info.last_timestamp;
    }
}

void Xdf::findMajSR()
{
    // find out which sample rate has the most channels
    typedef int sampRate;
    typedef int numChannel;

    std::vector<std::pair<sampRate, numChannel>> srateMap; //<srate, numchannels> pairs of all the streams

    //find out whether a sample rate already exists in srateMap
    for (auto const& [id, stream] : streams)
    {
        if (stream.info.nominal_srate != 0)
        {
            std::vector<std::pair<sampRate, numChannel>>::iterator it{
                std::find_if(srateMap.begin(), srateMap.end(),
                             [&](const std::pair<sampRate, numChannel>& element)
                             {
                                 return element.first == stream.info.nominal_srate;
                             })
            };
            //if it doesn't, add it here
            if (it == srateMap.end())
                srateMap.emplace_back(stream.info.nominal_srate, stream.info.channel_count);
            //if it already exists, add additional channel numbers to that sample rate
            else
            {
                int index(std::distance(srateMap.begin(), it));
                srateMap[index].second += stream.info.channel_count;
            }
        }
    }

    if (srateMap.size() > 0)
    {
        //search the srateMap to see which sample rate has the most channels
        int index(std::distance(srateMap.begin(),
                                std::max_element(srateMap.begin(), srateMap.end(),
                                                 [](const std::pair<sampRate, numChannel>& largest,
                                                    const std::pair<sampRate, numChannel>& first)
                                                 {
                                                     return largest.second < first.second;
                                                 })));

        majSR = srateMap[index].first; //the sample rate that has the most channels
    }
    else
    {
        majSR = 0; //if there are no streams with a fixed sample reate
    }
}

void Xdf::calcTotalChannel()
{
    //calculating total channel count, and indexing them onto streamMap
    for (size_t c = 0; c < streams.size(); c++)
    {
        if (!streams[c].time_series.empty())
        {
            totalCh += streams[c].info.channel_count;

            for (int i = 0; i < streams[c].info.channel_count; i++)
                streamMap.emplace_back(c);
        }
    }
}

void Xdf::calcTotalLength(int sampleRate)
{
    totalLen = (maxTS - minTS) * sampleRate;
}

void Xdf::freeUpTimeStamps()
{
    //free up as much memory as possible
    for (auto& stream : streams)
    {
        //we don't need to keep all the time stamps unless it's a stream with irregular samples
        //filter irregular streams and string streams
        if (stream.info.nominal_srate != 0 && !stream.time_stamps.empty() && stream.info.channel_format.
            compare("string"))
        {
            std::vector<double> nothing;
            //however we still need to keep the first time stamp of each stream to decide at which position the signal should start
            nothing.emplace_back(stream.time_stamps.front());
            stream.time_stamps.swap(nothing);
        }
    }
}

void Xdf::adjustTotalLength()
{
    for (auto const& stream : streams)
    {
        if (!stream.time_series.empty())
        {
            if (totalLen < stream.time_series.front().size())
                totalLen = stream.time_series.front().size();
        }
    }
}

void Xdf::getHighestSampleRate()
{
    for (auto const& stream : streams)
    {
        if (stream.info.nominal_srate > maxSR)
            maxSR = stream.info.nominal_srate;
    }
}

void Xdf::loadSampleRateMap()
{
    for (auto const& stream : streams)
        sampleRateMap.emplace(stream.info.nominal_srate);
}

/*
 void Xdf::detrend()
 {
 for (auto &stream : streams)
 {
 for (auto &row : stream.time_series)
 {
 long double init = 0.0;
 long double mean = std::accumulate(row.begin(), row.end(), init) / row.size();
 for(auto &val: row) val -= mean;
 offsets.emplace_back(mean);
 }
 }
 }
 */

void Xdf::calcEffectiveSrate()
{
    for (auto& stream : streams)
    {
        if (stream.info.nominal_srate)
        {
            try
            {
                stream.info.effective_sample_rate
                    = stream.info.sample_count /
                    (stream.info.last_timestamp - stream.info.first_timestamp);

                if (stream.info.effective_sample_rate)
                    effectiveSampleRateVector.emplace_back(stream.info.effective_sample_rate);

                pugi::xml_document doc;
                doc.load_string(stream.streamFooter.c_str());
                pugi::xml_node sampleCount = doc.child("info").child("sample_count");
                pugi::xml_node effectiveSampleRate
                    = doc.child("info").insert_child_after("effective_sample_rate", sampleCount);
                effectiveSampleRate.append_child(pugi::node_pcdata)
                                   .set_value(std::to_string(stream.info.effective_sample_rate).c_str());

                std::stringstream buffer;
                doc.save(buffer);

                stream.streamFooter = buffer.str();
            }
            catch (std::exception& e)
            {
                std::cerr << "Error calculating effective sample rate. "
                    << e.what() << std::endl;
            }
        }
    }
}

int Xdf::writeEventsToXDF(std::string file_path)
{
    if (userAddedStream)
    {
        std::fstream file;
        file.open(file_path, std::ios::app | std::ios::binary);

        if (file.is_open())
        {
            //start to append to new XDF file
            //first write a stream header chunk
            //Num Length Bytes
            file.put(4);
            //length
            int length = streams[userAddedStream].streamHeader.size() + 6;
            //+6 because of the length int itself and short int tag
            file.write((char*)&length, 4);

            //tag
            short tag = 2;
            file.write((char*)&tag, 2);
            //streamNumber
            int streamNumber = userAddedStream + 1; //+1 because the stream IDs in XDF are 1 based instead of 0 based
            file.write((char*)&streamNumber, 4);
            //content
            file.write(streams[userAddedStream].streamHeader.c_str(), length - 6); //length - 6 is the string length

            //write samples chunk
            //Num Length Bytes
            file.put(8);
            //length
            //add the bytes of all following actions together
            int64_t stringTotalLength = 0;
            for (auto const& event : userCreatedEvents)
                stringTotalLength += event.first.size();

            int64_t sampleChunkLength = 2 + 4 + 1 + 4 +
                userCreatedEvents.size() *
                (1 + 8 + 1 + 4) + stringTotalLength;
            file.write((char*)&sampleChunkLength, 8);


            //tag
            tag = 3;
            file.write((char*)&tag, 2);
            //streamNumber
            file.write((char*)&streamNumber, 4);
            //content
            //NumSamplesBytes
            file.put(4);

            //Num Samples
            int numSamples = userCreatedEvents.size();
            file.write((char*)&numSamples, 4);

            //samples
            for (auto const& event : userCreatedEvents)
            {
                //TimeStampBytes
                file.put(8);

                //Optional Time Stamp
                double timeStamp = event.second;
                file.write((char*)&timeStamp, 8);

                //Num Length Bytes
                file.put(4);

                //Length
                int stringLength = event.first.length();
                file.write((char*)&stringLength, 4);

                //String Content
                file.write(event.first.c_str(), stringLength);
            }

            file.close();
        }
        else
        {
            std::cerr << "Unable to open file." << std::endl;
            return -1; //Error
        }
    }

    std::cout << "Successfully wrote to XDF file." << std::endl;

    return 0; //Success
}

void Xdf::createLabels()
{
    size_t channelCount = 0;

    for (size_t st = 0; st < streams.size(); st++)
    {
        if (streams[st].info.channels.size())
        {
            for (size_t ch = 0; ch < streams[st].info.channels.size(); ch++)
            {
                // +1 for 1 based numbers; for user convenience only. The internal computation is still 0 based
                std::string label = "Stream " + std::to_string(st + 1) + " - Channel " + std::to_string(ch + 1)
                    + " - " + std::to_string((int)streams[st].info.nominal_srate) + " Hz\n";

                label += streams[st].info.name + '\n';

                for (auto const& entry : streams[st].info.channels[ch])
                {
                    if (entry.second != "")
                        label += entry.first + " : " + entry.second + '\n';
                }
                if (offsets.size())
                {
                    if (offsets[channelCount] >= 0)
                        label.append("baseline +").append(std::to_string(offsets[channelCount]));
                    else
                        label.append("baseline ").append(std::to_string(offsets[channelCount]));
                }
                labels.emplace_back(label);

                channelCount++;
            }
        }
        else
        {
            for (size_t ch = 0; ch < streams[st].time_series.size(); ch++)
            {
                // +1 for 1 based numbers; for user convenience only. The internal computation is still 0 based
                std::string label = "Stream " + std::to_string(st + 1) +
                    " - Channel " + std::to_string(ch + 1) + " - " +
                    std::to_string((int)streams[st].info.nominal_srate) +
                    " Hz\n" + streams[st].info.name + '\n' + streams[st].info.type + '\n';

                label += streams[st].info.name + '\n';

                if (offsets.size())
                {
                    if (offsets[channelCount] >= 0)
                        label.append("baseline +").append(std::to_string(offsets[channelCount]));
                    else
                        label.append("baseline ").append(std::to_string(offsets[channelCount]));
                }

                labels.emplace_back(label);

                channelCount++;
            }
        }
    }
}

void Xdf::loadDictionary()
{
    //loop through the eventMap
    for (auto const& entry : eventMap)
    {
        //search the dictionary to see whether an event is already in it
        auto it = std::find(dictionary.begin(), dictionary.end(), entry.first.first);
        //if it isn't yet
        if (it == dictionary.end())
        {
            //add it to the dictionary, also store its index into eventType vector for future use
            eventType.emplace_back(dictionary.size());
            dictionary.emplace_back(entry.first.first);
        }
        //if it's already in there
        else //store its index into eventType vector
            eventType.emplace_back(std::distance(dictionary.begin(), it));
    }
}

template <typename T>
T Xdf::readBin(std::istream& is, T* obj)
{
    T dummy;
    if (!obj) obj = &dummy;
    is.read(reinterpret_cast<char*>(obj), sizeof(T));
    return *obj;
}

} // namespace xdf
