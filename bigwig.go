// The MIT License (MIT)
// 
// Copyright (c) 2015 Bandwidth.com, Inc.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
    "fmt"
    "bufio"
    "os"
    "os/user"
    "log"
    "time"
    "strings"
    "container/list"
//    "container/ring"
    "regexp"
    "sort"
    "io"
    "io/ioutil"
    "path/filepath"
    "strconv"
)

type BigwigConfig struct {
    LinePattern *regexp.Regexp
    KeyPatterns []*regexp.Regexp
}

type LineData struct {
    Number int
    Position int64
    Length int64
}

type Line struct {
    LD LineData
    MatchText string
    FullText string
}

type KeysLineKV struct {
    Keys *list.List
    Value Line
}

type KeyLineMapData struct {
    LineNumberToKeyMap map[int]*list.List
    KeyToLineNumberMap map[string]*list.List
    LineNumberToLineData map[int]LineData
}

func NewKeyLineMapData() *KeyLineMapData {
    return &KeyLineMapData{
        make(map[int]*list.List),
        make(map[string]*list.List),
        make(map[int]LineData),
    }
}

func (m *KeyLineMapData) putLineNumberToKeyMap(lineno int, key string) {
    if m.LineNumberToKeyMap[lineno] == nil {
        m.LineNumberToKeyMap[lineno] = list.New()
    }
    m.LineNumberToKeyMap[lineno].PushBack(key)
}

func (m *KeyLineMapData) putKeyToLineNumberMap(key string, lineno int) {
    if m.KeyToLineNumberMap[key] == nil {
        m.KeyToLineNumberMap[key] = list.New()
    }
    m.KeyToLineNumberMap[key].PushBack(lineno)
}

func (m *KeyLineMapData) putLineNumberToLineData(lineno int, data LineData) {
    m.LineNumberToLineData[lineno] = data
}

func (m *KeyLineMapData) Put(kv *KeysLineKV) {
    if kv.Keys != nil {
        for e := kv.Keys.Front(); e != nil; e = e.Next() {
            k := e.Value.(string)
            m.putLineNumberToKeyMap(kv.Value.LD.Number, k)
            m.putKeyToLineNumberMap(k, kv.Value.LD.Number)
            m.putLineNumberToLineData(kv.Value.LD.Number, kv.Value.LD)
        }
    }
}

func (mapData *KeyLineMapData) SaveToIndexFile(filename string) {

    if mapData == nil {
        return
    }

    fmt.Printf("saving to index file %v\n", filename)

    file, err := os.Create(filename)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    writer := bufio.NewWriter(file)

    keyToSym := make(map[string]int)

    if mapData.KeyToLineNumberMap != nil {

        symValue := 1 // avoids "value, ok" lookup idiom
        for key, _ := range mapData.KeyToLineNumberMap {
            _, ok := keyToSym[key]
            if !ok {
                keyToSym[key] = symValue
                symValue++
            }
        }

        //fmt.Printf("keyToSym %v\n", keyToSym)

        writer.WriteString("=Symbols\n")
        for key, value := range keyToSym {
            writer.WriteString(strconv.Itoa(value))
            writer.WriteByte(',')
            writer.WriteString(key)
            writer.WriteByte('\n')
        }

        writer.WriteString("=KeyToLineNumberMap\n")
        for key, value := range mapData.KeyToLineNumberMap {
            for e := value.Front(); e != nil; e = e.Next() {
                if keyToSym[key] != 0 {
                    writer.WriteString(strconv.Itoa(keyToSym[key]))
                } else {
                    writer.WriteString(key)
                }
                writer.WriteByte(',')
                writer.WriteString(strconv.Itoa(e.Value.(int)))
                writer.WriteByte('\n')
            }
        }
    }

    if mapData.LineNumberToKeyMap != nil {
        writer.WriteString("=LineNumberToKeyMap\n")
        for key, value := range mapData.LineNumberToKeyMap {
            for e := value.Front(); e != nil; e = e.Next() {
                writer.WriteString(strconv.Itoa(key))
                writer.WriteByte(',')
                if keyToSym[e.Value.(string)] != 0 {
                    writer.WriteString(strconv.Itoa(keyToSym[e.Value.(string)]))
                } else {
                    writer.WriteString(e.Value.(string))
                }
                writer.WriteByte('\n')
            }
        }
    }

    if mapData.LineNumberToLineData != nil {
        writer.WriteString("=LineNumberToLineData\n")
        for key, value := range mapData.LineNumberToLineData {
            writer.WriteString(strconv.Itoa(key))
            writer.WriteByte(',')
            writer.WriteString(strconv.FormatInt(value.Position, 10))
            writer.WriteByte(',')
            writer.WriteString(strconv.FormatInt(value.Length, 10))
            writer.WriteByte('\n')
        }
    }

    writer.Flush()
}

func (mapData *KeyLineMapData) LoadFromIndexFile(filename string) *KeyLineMapData {

    fmt.Printf("loading from index file %v\n", filename)

    file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    sc := bufio.NewScanner(file)
    state := 0
    i := 1

    // Note that we cheat here by treating the syms as strings.
    symToKey := make(map[string]string)

    for sc.Scan() {

        text := strings.Trim(sc.Text(), " \r\n")

        if text == "=Symbols" {
            state = 1
            i++
            continue
        } else if text == "=LineNumberToKeyMap" {
            state = 2
            i++
            continue
        } else if text == "=KeyToLineNumberMap" {
            state = 3
            i++
            continue
        } else if text == "=LineNumberToLineData" {
            state = 4
            i++
            continue
        }

        switch state {

            case 1:
                kv := strings.Split(text, ",")
                if len(kv) < 2 {
                    log.Fatal("invalid index line ", i)
                }
                symToKey[kv[0]] = kv[1]

            case 2:
                kv := strings.Split(text, ",")
                if len(kv) < 2 {
                    log.Fatal("invalid index line ", i)
                }
                temp, err := strconv.Atoi(kv[0])
                if err == nil {
                    k := kv[1]
                    if symToKey[k] != "" {
                        k = symToKey[k]
                    }
                    mapData.putLineNumberToKeyMap(temp, k)
                } else {
                    log.Fatal("invalid index line ", i)
                }

            case 3:
                kv := strings.Split(text, ",")
                if len(kv) < 2 {
                    log.Fatal("invalid index line ", i)
                }
                temp, err := strconv.Atoi(kv[1])
                if err == nil {
                    k := kv[0]
                    if symToKey[k] != "" {
                        k = symToKey[k]
                    }
                    mapData.putKeyToLineNumberMap(k, temp)
                } else {
                    log.Fatal("invalid index line ", i)
                }

            case 4:
                kv := strings.Split(text, ",")
                if len(kv) < 3 {
                    log.Fatal("invalid index line ", i)
                }
                lineno, err1 := strconv.Atoi(kv[0])
                pos, err2 := strconv.Atoi(kv[1])
                length, err3 := strconv.Atoi(kv[2])
                if err1 != nil || err2 != nil || err3 != nil {
                    log.Fatal("invalid index line ", i)
                } else {
                    mapData.putLineNumberToLineData(lineno, LineData{lineno, int64(pos), int64(length)})
                }
        }

        i++
    }

    return nil
}

func (mapData *KeyLineMapData) doCorrelateKey(key string, corrLines map[int]int, corrKeys map[string]int) {

    if corrKeys[key] != 0 {
        return;
    }

    corrKeys[key] = 1
    fmt.Printf("correlate key %v\n", key)

    if mapData.KeyToLineNumberMap[key] != nil {
        for e := mapData.KeyToLineNumberMap[key].Front(); e != nil; e = e.Next() {
            mapData.correlateLine(key, e.Value.(int), corrLines, corrKeys)
        }
    }
}

func (mapData *KeyLineMapData) correlateLine(key string, lineno int, corrLines map[int]int, corrKeys map[string]int) {

    if corrLines[lineno] != 0 {
        return;
    }

    corrLines[lineno] = 1

    if mapData.LineNumberToKeyMap[lineno] != nil {
        for e := mapData.LineNumberToKeyMap[lineno].Front(); e != nil; e = e.Next() {
            if key != e.Value.(string) {
                mapData.doCorrelateKey(e.Value.(string), corrLines, corrKeys)
            }
        }
    }
}

func (mapData *KeyLineMapData) Correlate(key string) *list.List {

    //fmt.Printf("correlating %v\n", key)

    corrLines := make(map[int]int)
    corrKeys := make(map[string]int)

    mapData.doCorrelateKey(key, corrLines, corrKeys)

    temp := make([]int, len(corrLines))
    for k := range corrLines {
        temp = append(temp, k)
    }

    sort.Ints(temp)

    result := list.New()
    for l := 0; l < len(temp); l++ {
        value := temp[l]
        if value != 0 {
            result.PushBack(mapData.LineNumberToLineData[value])
        }
    }

    return result
}

func correlateAndPrint(filename string, correlationKey string, mapData *KeyLineMapData) {

    file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    start := time.Now()
    lines := mapData.Correlate(correlationKey)
    end := time.Now()
    fmt.Printf("correlated %v lines in %v\n", lines.Len(), end.Sub(start))

    if lines.Len() > 0 {
        fmt.Println()
        for e := lines.Front(); e != nil; e = e.Next() {
            ld := e.Value.(LineData)
            file.Seek(ld.Position, 0)
            data, err := ioutil.ReadAll(io.LimitReader(file, ld.Length))
            if err == nil {
                fmt.Printf("%v", string(data))
            }
        }
        fmt.Println()
    }
}

func initMapper(kvs chan *KeysLineKV, done chan *KeyLineMapData) {
    mapData := NewKeyLineMapData()
    go func() {
        for kv := range kvs {
            mapData.Put(kv)
        }
        done <- mapData
        close(done)
    }()
}

func initLineProcessor(config *BigwigConfig, extraKeys *list.List, lines chan *Line) chan *KeysLineKV {

    out := make(chan *KeysLineKV)

    extraPatterns := make([]*regexp.Regexp, extraKeys.Len())
    i := 0

    for e := extraKeys.Front(); e != nil; e = e.Next() {
        extraPatterns[i] = regexp.MustCompile(e.Value.(string))
        i++
    }

    go func() {
        for line := range lines {

            keys := list.New()

            for i := 0; i < len(config.KeyPatterns); i++ {
                matches := config.KeyPatterns[i].FindAllStringSubmatch(line.MatchText, -1)
                if len(matches) > 0 {
                    for j := 0; j < len(matches); j++ {
                        keys.PushBack(strings.Trim(matches[j][0], " "))
                    }
                }
            }

            for i = 0; i < len(extraPatterns); i++ {
                matches := extraPatterns[i].FindAllStringSubmatch(line.MatchText, -1)
                if len(matches) > 0 {
                    for j := 0; j < len(matches); j++ {
                        keys.PushBack(strings.Trim(matches[j][0], " "))
                    }
                }
            }

            out <- &KeysLineKV{keys, *line}
        }
        close(out)
    }()

    return out
}

func initMapModePipeline(config *BigwigConfig, extraKeys *list.List, lines chan *Line, done chan *KeyLineMapData) {
    lineProc := initLineProcessor(config, extraKeys, lines)
    initMapper(lineProc, done)
}

func buildLineFromText(linePattern *regexp.Regexp, lineno int, offset int64, lineLength int64, text string) *Line {
    var line *Line = nil
    match := linePattern.FindAllStringSubmatchIndex(text, -1)
    if len(match) > 0 && len(match[0]) > 1 {
        line = &Line{LineData{lineno, offset, lineLength}, text[match[0][0]:match[0][1]], text}
    }
    return line
}

func createMap(config *BigwigConfig, filename string, extraKeys *list.List) *KeyLineMapData {

    file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    fi, err := file.Stat()
    if err != nil {
        log.Fatal(err)
    }

    lineChannel := make(chan *Line)
    resultChannel := make(chan *KeyLineMapData)
    initMapModePipeline(config, extraKeys, lineChannel, resultChannel)

    fmt.Printf("scanning %v (%v bytes)\n", filename, fi.Size())

    lineno := 0
    reader := bufio.NewReader(file)
    var line *Line = nil

    start := time.Now()

    var offset int64 = 0
    buf, err := reader.ReadBytes('\n')

    for err == nil {

        lineLength := int64(len(buf))
        lineno++
        text := strings.Trim(string(buf), " \r\n")

        if len(text) > 0 {
            line = buildLineFromText(config.LinePattern, lineno, offset, lineLength, text)
            if line != nil {
                lineChannel <- line
            }
        }

        offset += lineLength
        buf, err = reader.ReadBytes('\n')
    }

    end := time.Now()

    close(lineChannel)

    mapData := <-resultChannel

    fmt.Printf("processed %v lines in %v\n", lineno, end.Sub(start))

    return mapData
}

func initLinePrinter(lines chan *Line) chan bool {

    out := make(chan bool)

    go func() {
        for l := range lines {
            fmt.Println(l.FullText)
        }
        close(out)
    }()

    return out
}

func initStepCorrelator(kvs chan *KeysLineKV, correlationKey string) chan *Line {

    out := make(chan *Line)
    mapData := NewKeyLineMapData()

    corrLines := make(map[int]int)
    corrKeys := make(map[string]int)

    go func() {
        for kv := range kvs {

            mapData.Put(kv)

            correlated := false

            for e := kv.Keys.Front(); e != nil; e = e.Next() {
                key := e.Value.(string)
                if key == correlationKey {
                    correlated = true
                    corrKeys[key] = 1
                } else if corrKeys[key] == 1 {
                    correlated = true
                }
            }

            if correlated {

                // all keys on this line are now correlated
                for e := kv.Keys.Front(); e != nil; e = e.Next() {

                    key := e.Value.(string)
                    corrKeys[key] = 1

                    // all lines known to contain any of these keys are now correlated
                    for e := mapData.KeyToLineNumberMap[key].Front(); e != nil; e = e.Next() {
                        corrLines[e.Value.(int)] = 1
                    }
                }

                out <- &kv.Value
            }
        }
        close(out)
    }()

    return out
}

func initStepModePipeline(config *BigwigConfig, extraKeys *list.List, correlationKey string, bufSize int, lines chan *Line) chan bool {
    kvChan := initLineProcessor(config, extraKeys, lines)
    corrKvChan := initStepCorrelator(kvChan, correlationKey)
    return initLinePrinter(corrKvChan)
}

func runStepMode(config *BigwigConfig, extraKeys *list.List, correlationKey string, bufSize int) {

    // TODO: buffer
//    fmt.Printf("running step mode with buffer size %v lines\n", bufSize)

    lineChannel := make(chan *Line)
    doneChannel := initStepModePipeline(config, extraKeys, correlationKey, bufSize, lineChannel)

//    rng := ring.New(bufSize)
//    useRing := true

    file := os.Stdin
    sc := bufio.NewScanner(file)
    lineno := 0
    var line *Line = nil

    for sc.Scan() {

        lineno++

        text := strings.Trim(sc.Text(), " \r\n")
        if len(text) == 0 {
            continue
        }

//        if useRing {
//            rng.Value = LineRingNode{lineno, text}
//            rng = rng.Next()
//        }

        line = buildLineFromText(config.LinePattern, lineno, 0, int64(len(text)), text)
        if line != nil {
            lineChannel <- line
        }
    }

    close(lineChannel)

    <-doneChannel
}

func loadConfig(filename string) BigwigConfig {

    linePatternString := `<[\d]+\.[\d]+\.[\d]+>@[\w-]+:[\w-]+:[\d]+ (.*)` 
    keyList := list.New()

    if filename == "" {

        // look in the current directory first
        filename = "./.bigwig.cfg"
        _, err := os.Stat(filename)
        if os.IsNotExist(err) {
            // now try the user's home directory
            u, err := user.Current()
            if err == nil {
                filename = u.HomeDir + "/.bigwig.cfg"
            } else {
                filename = ""
            }
        }

        if filename != "" {
            fmt.Printf("loading config file %v\n", filename)
        }
    }

    file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    sc := bufio.NewScanner(file)
    state := 0
    i := 1

    for sc.Scan() {

        text := strings.Trim(sc.Text(), " \r\n")
        if len(text) == 0 {
            i++
            continue
        }

        if text == "=LinePattern" {
            state = 1
            i++
            continue
        } else if text == "=KeyPatterns" {
            state = 2
            i++
            continue
        }

        switch state {

            case 1:
                linePatternString = text

            case 2:
                keyList.PushBack(text)
        }

        i++
    }

    linePattern := regexp.MustCompile(linePatternString)

    keyPatterns := make([]*regexp.Regexp, keyList.Len())
    i = 0

    for e := keyList.Front(); e != nil; e = e.Next() {
        keyPatterns[i] = regexp.MustCompile(e.Value.(string))
        i++
    }

    return BigwigConfig{linePattern, keyPatterns}
}

func main() {

    configFilename := ""
    filename := ""
    loadIndexFilename := ""
    saveIndex := false
    correlationKey := ""
    extraKeys := list.New()
    pipeMode := false
    bufSize := 1000

    if len(os.Args) > 1 {
        for argi := 1; argi < len(os.Args); argi++ {
            if "-c" == os.Args[argi] {
                argi++
                correlationKey = os.Args[argi]
            } else if "-s" == os.Args[argi] {
                saveIndex = true
            } else if "-l" == os.Args[argi] {
                argi++
                loadIndexFilename = os.Args[argi]
            } else if "-f" == os.Args[argi] {
                argi++
                filename = os.Args[argi]
            } else if "-k" == os.Args[argi] {
                argi++
                extraKeys.PushBack(os.Args[argi])
            } else if "-p" == os.Args[argi] {
                pipeMode = true
                argi++
                if argi < len(os.Args) {
                    temp, err := strconv.Atoi(os.Args[argi])
                    if err != nil {
                        argi--
                    } else {
                        bufSize = temp
                    }
                }
            } else if "--config" == os.Args[argi] {
                argi++
                configFilename = os.Args[argi]
            }
        }
    } else {
        return
    }

    config := loadConfig(configFilename)

    if pipeMode {
        if correlationKey != "" {
            runStepMode(&config, extraKeys, correlationKey, bufSize)
        } else {
            fmt.Printf("step mode requires a correlation key\n")
        }
        return
    }

    var mapData *KeyLineMapData = nil

    if loadIndexFilename != "" {
        mapData = NewKeyLineMapData()
        mapData.LoadFromIndexFile(loadIndexFilename)
    } else if filename != "" {
        mapData = createMap(&config, filename, extraKeys)
    } else {
        fmt.Printf("expecting index filename and/or filename argument\n")
        return
    }

    if saveIndex {
        indexFilename := filepath.Base(filename) + ".index"
        mapData.SaveToIndexFile(indexFilename)
    } 

    if len(correlationKey) > 0 {
        if filename != "" {
            correlateAndPrint(filename, correlationKey, mapData)
        } else {
            fmt.Printf("cannot print correlation results - missing filename argument\n")
        }
   }
}
