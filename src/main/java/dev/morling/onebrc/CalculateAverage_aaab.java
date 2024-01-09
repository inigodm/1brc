/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_aaab {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException {
        Date d = new Date();

        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        Collector<String[], MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    Double value = Double.parseDouble(m[1]);
                    if (value <= a.min) {
                        a.min = value;
                    } else {
                        if (value >= a.max) {
                            a.max = value;
                        }
                    }
                    a.sum += value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.min, agg.sum / agg.count, agg.max);
                });

        var executor = Executors.newVirtualThreadPerTaskExecutor();

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
                .parallel()
                .map(l -> l.split(";"))
                .collect(groupingBy(l -> l[0], collector)));

        System.out.println(STR."It take \{new Date().getTime() - d.getTime()} ms");
        System.out.println(measurements);

        System.out.println("It's ok? " + (measurements.toString().trim().equals("{Abha=-34.7/18.0/66.0, Abidjan=-24.8/26.0/72.7, Abéché=-20.2/29.4/76.3, Accra=-24.5/26.4/84.0, Addis Ababa=-31.5/16.0/65.8, Adelaide=-32.8/17.3/66.5, Aden=-19.2/29.1/77.5, Ahvaz=-23.1/25.4/73.8, Albuquerque=-39.5/14.0/65.3, Alexandra=-36.2/11.0/57.8, Alexandria=-28.2/20.0/72.8, Algiers=-32.6/18.2/66.9, Alice Springs=-33.6/21.0/68.9, Almaty=-37.3/10.0/58.1, Amsterdam=-40.1/10.2/62.1, Anadyr=-57.2/-6.9/43.0, Anchorage=-45.3/2.8/53.1, Andorra la Vella=-38.8/9.8/58.9, Ankara=-36.7/12.0/61.5, Antananarivo=-32.2/17.9/67.9, Antsiranana=-28.9/25.2/74.1, Arkhangelsk=-44.5/1.3/51.1, Ashgabat=-32.1/17.1/66.5, Asmara=-38.6/15.6/67.1, Assab=-16.5/30.5/78.5, Astana=-45.0/3.5/56.4, Athens=-32.9/19.2/71.7, Atlanta=-32.4/17.0/67.3, Auckland=-33.3/15.2/63.0, Austin=-30.3/20.7/68.4, Baghdad=-24.5/22.8/71.0, Baguio=-34.9/19.5/67.4, Baku=-38.1/15.1/66.3, Baltimore=-40.1/13.1/62.2, Bamako=-20.9/27.8/76.0, Bangkok=-25.1/28.6/80.7, Bangui=-27.2/26.0/76.8, Banjul=-24.8/26.0/78.4, Barcelona=-28.9/18.2/67.0, Bata=-21.3/25.1/73.4, Batumi=-34.7/14.0/69.9, Beijing=-37.1/12.9/61.7, Beirut=-30.5/20.9/71.1, Belgrade=-35.2/12.5/61.7, Belize City=-20.1/26.7/79.4, Benghazi=-27.9/19.9/68.4, Bergen=-48.0/7.7/58.1, Berlin=-39.7/10.3/61.2, Bilbao=-34.5/14.7/61.2, Birao=-25.5/26.5/76.3, Bishkek=-42.9/11.3/64.7, Bissau=-27.1/27.0/76.4, Blantyre=-27.1/22.2/70.8, Bloemfontein=-32.1/15.6/67.0, Boise=-35.5/11.4/62.9, Bordeaux=-34.3/14.2/64.4, Bosaso=-21.3/30.0/81.4, Boston=-42.1/10.9/58.4, Bouaké=-24.0/26.0/74.4, Bratislava=-37.3/10.5/62.9, Brazzaville=-24.1/25.0/71.2, Bridgetown=-21.3/27.0/75.7, Brisbane=-31.2/21.4/74.5, Brussels=-44.4/10.5/63.2, Bucharest=-37.9/10.8/61.3, Budapest=-34.8/11.3/60.0, Bujumbura=-25.3/23.8/75.3, Bulawayo=-32.4/18.9/67.5, Burnie=-35.2/13.1/67.8, Busan=-32.8/15.0/65.6, Cabo San Lucas=-27.3/23.9/75.1, Cairns=-26.5/25.0/80.6, Cairo=-27.1/21.4/70.5, Calgary=-45.5/4.4/55.5, Canberra=-33.0/13.1/63.0, Cape Town=-32.8/16.2/63.3, Changsha=-31.0/17.4/69.6, Charlotte=-35.0/16.1/63.5, Chiang Mai=-23.8/25.8/77.6, Chicago=-38.9/9.8/58.5, Chihuahua=-34.6/18.6/71.6, Chittagong=-25.8/25.9/73.1, Chișinău=-41.5/10.2/59.0, Chongqing=-31.9/18.6/70.0, Christchurch=-40.7/12.2/61.8, City of San Marino=-39.3/11.8/57.8, Colombo=-24.3/27.4/79.5, Columbus=-37.1/11.7/62.5, Conakry=-22.5/26.4/76.6, Copenhagen=-41.8/9.1/56.8, Cotonou=-24.3/27.2/78.0, Cracow=-40.2/9.3/59.5, Da Lat=-29.5/17.9/70.4, Da Nang=-24.9/25.8/80.2, Dakar=-30.5/24.0/70.9, Dallas=-35.4/19.0/69.4, Damascus=-30.8/17.0/70.4, Dampier=-21.9/26.4/74.5, Dar es Salaam=-26.5/25.8/71.8, Darwin=-20.7/27.6/76.7, Denpasar=-24.8/23.7/73.1, Denver=-38.3/10.4/62.5, Detroit=-37.0/10.0/58.0, Dhaka=-23.8/25.9/74.2, Dikson=-63.2/-11.1/37.0, Dili=-23.0/26.6/74.3, Djibouti=-19.0/29.9/79.7, Dodoma=-25.4/22.7/71.8, Dolisie=-26.1/24.0/75.1, Douala=-25.5/26.7/77.2, Dubai=-29.6/26.9/76.9, Dublin=-41.9/9.8/62.9, Dunedin=-41.4/11.1/62.2, Durban=-28.7/20.6/70.6, Dushanbe=-38.6/14.7/61.2, Edinburgh=-48.9/9.3/62.4, Edmonton=-45.4/4.2/53.3, El Paso=-30.8/18.1/69.7, Entebbe=-27.2/21.0/69.3, Erbil=-30.6/19.5/70.0, Erzurum=-45.8/5.1/54.8, Fairbanks=-51.4/-2.3/54.6, Fianarantsoa=-29.5/17.9/65.6, Flores,  Petén=-25.3/26.4/75.5, Frankfurt=-41.2/10.6/66.9, Fresno=-34.3/17.9/65.6, Fukuoka=-33.5/17.0/65.1, Gaborone=-30.7/21.0/68.5, Gabès=-30.4/19.5/67.6, Gagnoa=-25.3/26.0/75.5, Gangtok=-36.5/15.2/66.4, Garissa=-21.1/29.3/80.8, Garoua=-24.1/28.3/76.5, George Town=-21.0/27.9/77.6, Ghanzi=-29.8/21.4/74.1, Gjoa Haven=-60.9/-14.4/33.3, Guadalajara=-30.4/20.9/70.7, Guangzhou=-28.2/22.4/71.4, Guatemala City=-30.3/20.4/69.5, Halifax=-44.1/7.5/56.1, Hamburg=-44.1/9.7/58.9, Hamilton=-34.6/13.8/66.0, Hanga Roa=-29.1/20.5/70.5, Hanoi=-28.1/23.6/76.5, Harare=-30.1/18.4/71.9, Harbin=-46.6/5.0/55.3, Hargeisa=-28.6/21.7/69.8, Hat Yai=-20.6/27.0/78.8, Havana=-24.5/25.2/74.7, Helsinki=-44.8/5.9/55.3, Heraklion=-30.7/18.9/66.3, Hiroshima=-36.2/16.3/65.5, Ho Chi Minh City=-21.2/27.4/77.0, Hobart=-36.7/12.7/64.8, Hong Kong=-24.7/23.3/76.6, Honiara=-29.0/26.5/74.3, Honolulu=-25.4/25.4/73.3, Houston=-27.2/20.8/70.3, Ifrane=-38.5/11.4/62.2, Indianapolis=-39.4/11.8/60.0, Iqaluit=-59.5/-9.3/41.9, Irkutsk=-46.9/1.0/53.5, Istanbul=-34.0/13.9/60.9, Jacksonville=-28.1/20.3/69.3, Jakarta=-23.5/26.7/80.4, Jayapura=-22.2/27.0/79.2, Jerusalem=-33.4/18.3/67.0, Johannesburg=-32.3/15.5/71.8, Jos=-26.4/22.8/69.4, Juba=-19.0/27.8/77.2, Kabul=-35.9/12.1/60.3, Kampala=-29.1/20.0/70.2, Kandi=-22.8/27.7/75.7, Kankan=-22.6/26.5/76.3, Kano=-24.2/26.4/76.8, Kansas City=-42.5/12.5/60.8, Karachi=-27.7/26.0/73.7, Karonga=-26.8/24.4/73.4, Kathmandu=-33.2/18.3/65.8, Khartoum=-21.4/29.9/82.6, Kingston=-29.2/27.4/82.9, Kinshasa=-27.7/25.3/75.7, Kolkata=-24.2/26.7/78.4, Kuala Lumpur=-23.0/27.3/84.5, Kumasi=-27.7/26.0/77.0, Kunming=-35.2/15.7/64.5, Kuopio=-47.0/3.4/54.5, Kuwait City=-24.3/25.7/75.2, Kyiv=-46.0/8.4/58.8, Kyoto=-33.8/15.8/64.2, La Ceiba=-22.3/26.2/76.2, La Paz=-28.8/23.7/72.8, Lagos=-22.9/26.8/77.8, Lahore=-24.3/24.3/74.8, Lake Havasu City=-29.6/23.7/75.9, Lake Tekapo=-41.5/8.7/59.2, Las Palmas de Gran Canaria=-30.9/21.2/68.0, Las Vegas=-32.9/20.3/68.9, Launceston=-36.3/13.1/62.6, Lhasa=-41.7/7.6/57.2, Libreville=-20.0/25.9/76.9, Lisbon=-29.8/17.5/66.1, Livingstone=-24.9/21.8/71.7, Ljubljana=-38.7/10.9/59.4, Lodwar=-18.6/29.3/80.3, Lomé=-26.1/26.9/79.6, London=-37.3/11.3/60.3, Los Angeles=-33.3/18.6/65.5, Louisville=-35.7/13.9/68.6, Luanda=-29.4/25.8/75.8, Lubumbashi=-29.9/20.8/69.7, Lusaka=-32.4/19.9/68.6, Luxembourg City=-40.7/9.3/57.6, Lviv=-44.9/7.8/60.5, Lyon=-38.1/12.5/65.5, Madrid=-32.2/15.0/65.1, Mahajanga=-26.4/26.3/85.3, Makassar=-21.7/26.7/75.1, Makurdi=-24.7/26.0/73.9, Malabo=-23.5/26.3/79.1, Malé=-23.7/28.0/78.7, Managua=-23.6/27.3/78.6, Manama=-25.5/26.5/75.3, Mandalay=-19.2/28.0/76.9, Mango=-21.1/28.1/76.8, Manila=-22.4/28.4/80.5, Maputo=-23.3/22.8/70.1, Marrakesh=-32.1/19.6/68.9, Marseille=-34.1/15.8/65.9, Maun=-29.0/22.4/71.2, Medan=-23.3/26.5/73.1, Mek'ele=-25.7/22.7/73.0, Melbourne=-32.3/15.1/66.3, Memphis=-29.2/17.2/65.4, Mexicali=-25.1/23.1/77.6, Mexico City=-31.2/17.5/67.1, Miami=-24.1/24.9/74.6, Milan=-37.0/13.0/63.6, Milwaukee=-45.2/8.9/56.0, Minneapolis=-40.2/7.8/58.3, Minsk=-42.3/6.7/60.8, Mogadishu=-30.6/27.1/75.0, Mombasa=-27.0/26.3/78.0, Monaco=-34.9/16.4/68.7, Moncton=-47.9/6.1/58.3, Monterrey=-29.4/22.3/74.0, Montreal=-43.7/6.8/55.5, Moscow=-45.9/5.8/55.6, Mumbai=-23.1/27.1/77.8, Murmansk=-50.3/0.6/49.5, Muscat=-27.1/28.0/81.4, Mzuzu=-30.0/17.7/68.5, N'Djamena=-21.1/28.3/79.9, Naha=-26.1/23.1/74.8, Nairobi=-31.1/17.8/66.8, Nakhon Ratchasima=-23.5/27.3/75.2, Napier=-38.4/14.6/64.8, Napoli=-31.3/15.9/66.8, Nashville=-35.3/15.4/63.1, Nassau=-26.9/24.6/76.4, Ndola=-29.8/20.3/68.9, New Delhi=-24.2/25.0/77.8, New Orleans=-30.7/20.7/69.3, New York City=-39.5/12.9/60.8, Ngaoundéré=-24.7/22.0/70.0, Niamey=-22.2/29.3/80.5, Nicosia=-30.2/19.7/69.5, Niigata=-32.8/13.9/62.9, Nouadhibou=-27.0/21.3/71.1, Nouakchott=-22.3/25.7/73.5, Novosibirsk=-49.1/1.7/58.9, Nuuk=-52.4/-1.4/47.1, Odesa=-38.2/10.7/60.8, Odienné=-24.5/26.0/73.4, Oklahoma City=-35.8/15.9/66.4, Omaha=-38.5/10.6/60.6, Oranjestad=-19.3/28.1/76.9, Oslo=-47.0/5.7/56.3, Ottawa=-42.9/6.6/59.1, Ouagadougou=-21.0/28.3/76.8, Ouahigouya=-22.2/28.6/79.6, Ouarzazate=-29.5/18.9/66.7, Oulu=-46.1/2.7/53.4, Palembang=-21.4/27.3/76.2, Palermo=-34.8/18.5/68.8, Palm Springs=-26.3/24.5/72.4, Palmerston North=-34.9/13.2/68.9, Panama City=-23.5/28.0/77.7, Parakou=-21.5/26.8/79.2, Paris=-39.5/12.3/63.1, Perth=-39.0/18.7/75.8, Petropavlovsk-Kamchatsky=-47.7/1.9/51.2, Philadelphia=-45.6/13.2/62.3, Phnom Penh=-22.8/28.3/78.1, Phoenix=-24.3/23.9/75.0, Pittsburgh=-39.8/10.8/58.6, Podgorica=-36.8/15.3/64.6, Pointe-Noire=-24.5/26.1/76.1, Pontianak=-21.5/27.7/76.5, Port Moresby=-23.5/26.9/74.1, Port Sudan=-24.8/28.4/77.1, Port Vila=-26.3/24.3/71.0, Port-Gentil=-26.7/26.0/77.7, Portland (OR)=-39.1/12.4/67.2, Porto=-33.8/15.7/68.1, Prague=-41.6/8.4/57.6, Praia=-30.0/24.4/73.4, Pretoria=-32.2/18.2/74.6, Pyongyang=-38.2/10.8/59.9, Rabat=-28.9/17.2/66.7, Rangpur=-24.2/24.4/73.1, Reggane=-19.1/28.3/78.0, Reykjavík=-47.2/4.3/53.1, Riga=-47.7/6.2/57.0, Riyadh=-22.5/26.0/80.6, Rome=-35.3/15.2/64.0, Roseau=-23.3/26.2/79.5, Rostov-on-Don=-40.8/9.9/59.5, Sacramento=-40.1/16.3/66.7, Saint Petersburg=-43.6/5.8/54.0, Saint-Pierre=-42.0/5.7/62.0, Salt Lake City=-40.0/11.6/60.8, San Antonio=-34.7/20.8/69.7, San Diego=-34.1/17.8/66.0, San Francisco=-44.8/14.6/70.1, San Jose=-33.4/16.4/67.7, San José=-30.3/22.6/71.9, San Juan=-26.0/27.2/74.4, San Salvador=-24.0/23.1/69.8, Sana'a=-30.6/20.0/72.9, Santo Domingo=-29.1/25.9/77.7, Sapporo=-43.7/8.9/55.3, Sarajevo=-37.8/10.1/59.6, Saskatoon=-46.5/3.3/52.1, Seattle=-36.8/11.3/60.3, Seoul=-36.6/12.5/62.0, Seville=-29.8/19.2/73.4, Shanghai=-30.5/16.7/74.7, Singapore=-26.2/27.0/77.2, Skopje=-35.3/12.4/64.8, Sochi=-36.3/14.2/67.2, Sofia=-37.6/10.6/59.5, Sokoto=-23.3/28.0/77.8, Split=-31.7/16.1/69.1, St. John's=-47.4/5.0/55.1, St. Louis=-36.2/13.9/64.7, Stockholm=-43.8/6.6/54.2, Surabaya=-19.9/27.1/79.2, Suva=-27.6/25.6/72.6, Suwałki=-43.4/7.2/56.4, Sydney=-33.3/17.7/69.4, Ségou=-21.7/28.0/77.5, Tabora=-27.6/23.0/71.6, Tabriz=-39.4/12.6/62.2, Taipei=-30.4/23.0/72.7, Tallinn=-41.5/6.4/59.3, Tamale=-22.6/27.9/84.3, Tamanrasset=-26.9/21.7/70.3, Tampa=-31.6/22.9/76.0, Tashkent=-34.8/14.8/65.4, Tauranga=-35.6/14.8/64.7, Tbilisi=-36.5/12.9/62.9, Tegucigalpa=-28.8/21.7/70.8, Tehran=-32.1/17.0/70.5, Tel Aviv=-31.4/20.0/68.7, Thessaloniki=-36.0/16.0/69.8, Thiès=-25.1/24.0/71.7, Tijuana=-33.1/17.8/64.7, Timbuktu=-21.6/28.0/77.0, Tirana=-37.1/15.2/64.3, Toamasina=-23.4/23.4/73.4, Tokyo=-34.0/15.4/62.5, Toliara=-23.4/24.1/75.3, Toluca=-38.2/12.4/62.4, Toronto=-38.7/9.4/59.8, Tripoli=-28.8/20.0/72.9, Tromsø=-48.0/2.9/53.5, Tucson=-26.8/20.9/70.1, Tunis=-28.7/18.4/65.0, Ulaanbaatar=-49.1/-0.4/53.2, Upington=-28.4/20.4/70.7, Vaduz=-44.5/10.1/62.8, Valencia=-33.0/18.3/67.5, Valletta=-28.7/18.8/69.8, Vancouver=-45.7/10.4/59.3, Veracruz=-24.8/25.4/78.9, Vienna=-40.0/10.4/59.7, Vientiane=-21.7/25.9/75.3, Villahermosa=-20.6/27.1/76.4, Vilnius=-45.3/6.0/56.3, Virginia Beach=-32.0/15.8/66.0, Vladivostok=-43.4/4.9/57.6, Warsaw=-38.6/8.5/55.5, Washington, D.C.=-33.8/14.6/70.8, Wau=-25.0/27.8/82.6, Wellington=-36.7/12.9/64.7, Whitehorse=-49.5/-0.1/47.4, Wichita=-34.2/13.9/62.3, Willemstad=-22.6/28.0/78.1, Winnipeg=-45.8/3.0/54.6, Wrocław=-44.9/9.6/64.0, Xi'an=-45.2/14.1/64.1, Yakutsk=-55.9/-8.8/49.9, Yangon=-23.1/27.5/75.9, Yaoundé=-28.7/23.8/75.7, Yellowknife=-53.2/-4.3/43.5, Yerevan=-41.5/12.4/64.1, Yinchuan=-40.1/9.0/63.8, Zagreb=-43.8/10.7/59.0, Zanzibar City=-22.3/26.0/74.4, Zürich=-41.4/9.3/59.0, Ürümqi=-41.6/7.4/60.9, İzmir=-32.2/17.9/76.4}")));

    }

    private String[] split(char ch, int limit, boolean withDelimiters, String s) {
        int matchCount = 0;
        int off = 0;
        int next;
        boolean limited = limit > 0;
        ArrayList<String> list = new ArrayList<>();
        String del = withDelimiters ? String.valueOf(ch) : null;
        while ((next = s.indexOf(ch, off)) != -1) {
            if (!limited || matchCount < limit - 1) {
                list.add(s.substring(off, next));
                if (withDelimiters) {
                    list.add(del);
                }
                off = next + 1;
                ++matchCount;
            }
            else { // last one
                int last = s.length();
                list.add(s.substring(off, last));
                off = last;
                ++matchCount;
                break;
            }
        }
        // If no match was found, return this
        if (off == 0)
            return new String[]{ s };

        // Add remaining segment
        if (!limited || matchCount < limit)
            list.add(s.substring(off, s.length()));

        // Construct result
        int resultSize = list.size();
        if (limit == 0) {
            while (resultSize > 0 && list.get(resultSize - 1).isEmpty()) {
                resultSize--;
            }
        }
        String[] result = new String[resultSize];
        return list.subList(0, resultSize).toArray(result);
    }

}
