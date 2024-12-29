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

/*!
 * \brief Read a binary scalar variable from an input stream.
 *
 * read_bin is a convenience wrapper for the common
 * file.read((char*) var, sizeof(var))
 * operation. Examples:
 * double foo = read_bin<double>(file); // use return value
 * read_bin(file, &foo); // read directly into foo
 * \param is an input stream to read from
 * \param obj pointer to a variable to load the data into or nullptr
 * \return the read data
 */
template <typename T>
[[nodiscard]] T read_bin(std::istream& is)
{
    T obj;
    is.read(reinterpret_cast<char*>(&obj), sizeof(T));
    return obj;
}


/*!
 * \brief Returns the length of the upcoming chunk, or the number of samples.
 *
 * While loading XDF file there are 2 cases where this function will be
 * needed. One is to get the length of each chunk, one is to get the
 * number of samples when loading the time series (Chunk tag 3).
 * \param file is the XDF file that is being loaded in the type of `std::ifstream`.
 * \return The length of the upcoming chunk (in bytes).
 */
uint64_t read_length(std::istream& is)
{
    switch (const auto bytes = read_bin<uint8_t>(is); bytes)
    {
    case 1:
        return read_bin<uint8_t>(is);
    case 4:
        return read_bin<uint32_t>(is);
    case 8:
        return read_bin<uint64_t>(is);
    default:
        throw std::runtime_error("Invalid variable-length integer length: "
                                 + std::to_string(static_cast<int>(bytes)));
    }
}

template <typename T>
void read_time_series(std::istream& is, std::vector<std::vector<T>>* time_series) {
    if constexpr (std::is_same_v<T, std::string>) {
        for (std::vector<std::string>& row : *time_series)
        {
            const uint64_t length = read_length(is);
            char* buffer = new char[length + 1];
            is.read(buffer, length);
            buffer[length] = '\0';
            row.emplace_back(buffer);
            delete[] buffer;
        }
        return;
    }
    for (std::vector<T>& row : *time_series)
    {
        row.push_back(std::move(read_bin<T>(is)));
    }
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

    std::vector<int> idmap; //remaps stream id's onto indices in streams


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
            uint64_t ChLen = read_length(file); //chunk length

            if (ChLen == 0)
                break;

            //read tag of the chunk, 6 possibilities
            switch (const auto tag = read_bin<uint16_t>(file); tag)
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
                    const auto stream_id = read_bin<uint32_t>(file);
                    int index;

                    std::vector<int>::iterator it{std::find(idmap.begin(), idmap.end(), stream_id)};
                    if (it == idmap.end())
                    {
                        index = idmap.size();
                        idmap.emplace_back(stream_id);
                        streams.emplace_back();
                    }
                    else
                        index = std::distance(idmap.begin(), it);


                    pugi::xml_document doc;

                    //read [Content]
                    char* buffer = new char[ChLen - 6];
                    file.read(buffer, ChLen - 6);
                    streams[index].streamHeader = buffer;

                    doc.load_buffer_inplace(buffer, ChLen - 6);

                    pugi::xml_node info = doc.child("info");
                    pugi::xml_node desc = info.child("desc");

                    streams[index].info.channel_count = info.child("channel_count").text().as_int();
                    streams[index].info.nominal_srate = info.child("nominal_srate").text().as_double();
                    streams[index].info.name = info.child("name").text().get();
                    streams[index].info.type = info.child("type").text().get();
                    streams[index].info.channel_format = info.child("channel_format").text().get();

                    for (auto channel = desc.child("channels").child("channel"); channel; channel = channel.
                         next_sibling("channel"))
                    {
                        streams[index].info.channels.emplace_back();

                        for (auto const& entry : channel.children())
                            streams[index].info.channels.back().emplace(entry.name(), entry.child_value());
                    }

                    if (streams[index].info.nominal_srate > 0)
                        streams[index].sampling_interval = 1 / streams[index].info.nominal_srate;
                    else
                        streams[index].sampling_interval = 0;

                    delete[] buffer;

                    Stream& stream = streams[index];
                    const int channel_count = stream.info.channel_count;
                    if (const std::string_view channel_format = stream.info.channel_format;
                            channel_format == "string")
                    {
                        stream.time_series.emplace<std::vector<std::vector<std::string>>>(
                            channel_count);
                    }
                    else if (channel_format == "float32")
                    {
                        stream.time_series.emplace<std::vector<std::vector<float>>>(
                            channel_count);
                    }
                    else if (channel_format == "double64")
                    {
                        stream.time_series.emplace<std::vector<std::vector<double>>>(
                            channel_count);
                    }
                    else if (channel_format == "int8_t")
                    {
                        stream.time_series.emplace<std::vector<std::vector<int8_t>>>(
                            channel_count);
                    }
                    else if (channel_format == "int16_t")
                    {
                        stream.time_series.emplace<std::vector<std::vector<int16_t>>>(
                            channel_count);
                    }
                    else if (channel_format == "int32_t")
                    {
                        stream.time_series.emplace<std::vector<std::vector<int>>>(
                            channel_count);
                    }
                    else if (channel_format == "int64_t")
                    {
                        stream.time_series.emplace<std::vector<std::vector<int64_t>>>(
                            channel_count);
                    }
                    else {
                        throw std::runtime_error("Unexpected channel format: "
                                                 + std::string(channel_format));
                    }
                }
                break;
            case 3: //read [Samples] chunk
                {
                    //read [StreamID]
                    const auto stream_id = read_bin<uint32_t>(file);
                    int index;
                    std::vector<int>::iterator it{std::find(idmap.begin(), idmap.end(), stream_id)};
                    if (it == idmap.end())
                    {
                        index = idmap.size();
                        idmap.emplace_back(stream_id);
                        streams.emplace_back();
                    }
                    else
                        index = std::distance(idmap.begin(), it);


                    //read [NumSampleBytes], [NumSamples]
                    uint64_t numSamp = read_length(file);

                    //for each sample
                    for (size_t i = 0; i < numSamp; i++)
                    {
                        //read or deduce time stamp
                        const auto tsBytes = read_bin<uint8_t>(file);

                        double ts; //temporary time stamp

                        if (tsBytes == 8)
                        {
                            ts = read_bin<double>(file);
                            streams[index].time_stamps.emplace_back(ts);
                        }
                        else
                        {
                            ts = streams[index].last_timestamp + streams[index].sampling_interval;
                            streams[index].time_stamps.emplace_back(ts);
                        }

                        streams[index].last_timestamp = ts;

                        std::visit([&file](auto&& time_series) {
                            using T = typename std::remove_reference_t
                                <decltype(time_series)>::value_type::value_type;
                            read_time_series<T>(file, &time_series);
                        }, streams[index].time_series);
                    }
                }
                break;
            case 4: //read [ClockOffset] chunk
                {
                    const auto stream_id = read_bin<uint32_t>(file);
                    int index;
                    std::vector<int>::iterator it{std::find(idmap.begin(), idmap.end(), stream_id)};
                    if (it == idmap.end())
                    {
                        index = idmap.size();
                        idmap.emplace_back(stream_id);
                        streams.emplace_back();
                    }
                    else
                        index = std::distance(idmap.begin(), it);

                    const auto collection_time = read_bin<double>(file);
                    const auto offset_value = read_bin<double>(file);

                    streams[index].clock_times.push_back(collection_time);
                    streams[index].clock_values.push_back(offset_value);
                }
                break;
            case 6: //read [StreamFooter] chunk
                {
                    pugi::xml_document doc;

                    //read [StreamID]
                    const auto stream_id = read_bin<uint32_t>(file);
                    int index;
                    std::vector<int>::iterator it{std::find(idmap.begin(), idmap.end(), stream_id)};
                    if (it == idmap.end())
                    {
                        index = idmap.size();
                        idmap.emplace_back(stream_id);
                        streams.emplace_back();
                    }
                    else
                        index = std::distance(idmap.begin(), it);


                    char* buffer = new char[ChLen - 6];
                    file.read(buffer, ChLen - 6);
                    streams[index].streamFooter = buffer;

                    doc.load_buffer_inplace(buffer, ChLen - 6);

                    pugi::xml_node info = doc.child("info");

                    streams[index].info.first_timestamp = info.child("first_timestamp").text().as_double();
                    streams[index].info.last_timestamp = info.child("last_timestamp").text().as_double();
                    streams[index].info.measured_srate = info.child("measured_srate").text().as_double();
                    streams[index].info.sample_count = info.child("sample_count").text().as_int();
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
    for (auto& stream : this->streams)
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
        if (streams[k].info.channel_format == "string")
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
    for (Stream& stream : streams)
    {
        if (stream.time_series.index() != std::variant_npos &&
            stream.info.channel_format != "string" &&
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

            std::visit([&pfilt, &pstate](auto&& time_series) {
                using T = typename std::remove_reference_t<decltype(time_series)>
                    ::value_type::value_type;
                for (std::vector<T>& row : time_series)
                {
                    // initialize buffers
                    int read = 0;
                    int written = 0;
                    const int OUT_BUF_SIZE = (int)smarc_get_output_buffer_size(pfilt, row.size());
                    double* inbuf = new double[row.size()];
                    double* outbuf = new double[OUT_BUF_SIZE];

                    // Fill inbuf with the numeric values from the row
                    if constexpr (std::is_arithmetic_v<T>)
                    {
                        for (const T& val : row)
                        {
                            inbuf[read++] = static_cast<double>(val); // Convert to double
                        }
                    }

                    // resample signal block
                    written = smarc_resample(pfilt, pstate, inbuf, read, outbuf, OUT_BUF_SIZE);

                    // Replace original values with the resampled output
                    read = 0;
                    // Only replace numeric values
                    if constexpr (std::is_arithmetic_v<T>)
                    {
                        for (auto& val : row)
                        {
                            val = static_cast<T>(outbuf[read++]);
                        }
                    }

                    // flushing last values
                    written = smarc_resample_flush(pfilt, pstate, outbuf, OUT_BUF_SIZE);

                    // Add any remaining flushed values
                    read = 0;
                    // Only replace numeric values
                    if constexpr (std::is_arithmetic_v<T>)
                    {
                        for (auto& val : row)
                        {
                            val = static_cast<T>(outbuf[read++]);
                        }
                    }

                    // you are done with converting your signal.
                    // If you want to reuse the same converter to process another signal
                    // just reset the state:
                    smarc_reset_pstate(pstate, pfilt);

                    delete[] inbuf;
                    delete[] outbuf;
                }
            }, stream.time_series);

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

void Xdf::findMinMax()
{
    //find the smallest timestamp of all streams
    for (auto const& stream : streams)
    {
        if (!std::isnan(stream.info.first_timestamp))
        {
            minTS = stream.info.first_timestamp;
            break;
        }
    }
    for (auto const& stream : streams)
    {
        if (!std::isnan(stream.info.first_timestamp) && stream.info.first_timestamp < minTS)
            minTS = stream.info.first_timestamp;
    }

    //find the max timestamp of all streams
    for (auto const& stream : streams)
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
    for (auto const& stream : streams)
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
        if (streams[c].time_series.index() != std::variant_npos &&
            streams[c].info.channel_format != "string")
        {
            numerical_channel_count_ += streams[c].info.channel_count;

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
        if (stream.time_series.index() != std::variant_npos &&
            stream.info.channel_format != "string")
        {
            std::visit([this](auto&& time_series) {
                totalLen = std::max(totalLen, time_series.front().size());
            }, stream.time_series);
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


void Xdf::detrend()
{
    for (Stream &stream : streams)
    {
        std::visit([this](auto&& time_series) {
            using T = typename std::remove_reference_t<decltype(time_series)>
                ::value_type::value_type;
            if constexpr (std::is_arithmetic_v<T>) {
                for (auto& row : time_series)
                {
                    long double init = 0.0;
                    long double mean = std::accumulate(row.begin(), row.end(), init) / row.size();
                    for (auto& val: row)
                    {
                        val -= mean;
                    }
                    offsets.push_back(mean);
                }
            }
        }, stream.time_series);
    }
}


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
            for (size_t ch = 0; ch < streams[st].info.channel_count; ch++)
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

} // namespace xdf
