// timescaledb-parallel-copy loads data from CSV format into a TimescaleDB database
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Flag vars
var (
	postgresConnect string
	dbName          string
	schemaName      string
	tableName       string
	truncate        bool

	copyOptions    string
	splitCharacter string
	fromFile       string
	columns        string

	workers         int
	batchSize       int
	logBatches      bool
	reportingPeriod time.Duration
	verbose         bool

	columnCount int64
	rowCount    int64

	durasi time.Time
	
	f *os.File
)

type batch struct {
	rows []string
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

// Parse args
func init() {
	flag.StringVar(&postgresConnect, "connection", "host=localhost user=postgres sslmode=disable", "PostgreSQL connection url")
	flag.StringVar(&dbName, "db-name", "test", "Database where the destination table exists")
	flag.StringVar(&tableName, "table", "test_table", "Destination table for insertions")
	flag.StringVar(&schemaName, "schema", "public", "Desination table's schema")
	flag.BoolVar(&truncate, "truncate", false, "Truncate the destination table before insert")

	flag.StringVar(&copyOptions, "copy-options", "", "Additional options to pass to COPY (ex. NULL 'NULL')")
	flag.StringVar(&splitCharacter, "split", ",", "Character to split by")
	flag.StringVar(&fromFile, "file", "", "File to read from rather than stdin")
	flag.StringVar(&columns, "columns", "", "Comma-separated columns present in CSV")

	flag.IntVar(&batchSize, "batch-size", 5000, "Number of rows per insert")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make")
	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.DurationVar(&reportingPeriod, "reporting-period", 0*time.Second, "Period to report insert stats; if 0s, intermediate results will not be reported")
	flag.BoolVar(&verbose, "verbose", false, "Print more information about copying statistics")

	flag.Parse()
}

func getConnectString() string {
	return fmt.Sprintf("%s dbname=%s", postgresConnect, dbName)
}

func getFullTableName() string {
	return fmt.Sprintf("\"%s\".\"%s\"", schemaName, tableName)
}

func main() {

	f, _ = os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()

	if truncate { // Remove existing data from the table
		dbBench := sqlx.MustConnect("postgres", getConnectString())
		_, err := dbBench.Exec(fmt.Sprintf("TRUNCATE %s", getFullTableName()))
		if err != nil {
			panic(err)
		}

		err = dbBench.Close()
		if err != nil {
			panic(err)
		}
	}

	var scanner *bufio.Scanner
	if len(fromFile) > 0 {
		file, err := os.Open(fromFile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner = bufio.NewScanner(file)
	} else {
		scanner = bufio.NewScanner(os.Stdin)
	}

	var wg sync.WaitGroup
	batchChan := make(chan *batch, workers)

	// Generate COPY workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go processBatches(&wg, batchChan)
	}

	// Reporting thread
	if reportingPeriod > (0 * time.Second) {
		go report()
	}

	start := time.Now()
	rowsRead := scan(batchSize, scanner, batchChan)
	close(batchChan)
	wg.Wait()
	end := time.Now()
	took := end.Sub(start)
	rowRate := float64(rowsRead) / float64(took.Seconds())

	res := fmt.Sprintf("COPY %d", rowsRead)
	if verbose {
		res += fmt.Sprintf(", took %v with %d worker(s) (mean rate %f/sec)", took, workers, rowRate)
	}
	fmt.Println(res)
	
	fmt.Println("Moving data into hourly table")
	dbBench := sqlx.MustConnect("postgres", getConnectString())
	defer dbBench.Close()
	dbBench.MustExec("insert into counter_4g_h select * from counter_4g_ld on conflict do nothing; insert into counter_4g_daily select time_bucket('1 day',RESULTTIME) tanggal,unique_id,NENAME,ENODEBFUNCTIONNAME,LOCALCELLID,CELLNAME,ENODEBID,CELLFDDTDDINDICATION,sum(LCellUnavailDurSys),sum(LChMeasCCEAvail),sum(LChMeasCCECommUsed),sum(LChMeasCCEDLUsed),sum(LChMeasCCEULUsed),sum(LChMeasCQIDL0),sum(LChMeasCQIDL1),sum(LChMeasCQIDL10),sum(LChMeasCQIDL11),sum(LChMeasCQIDL12),sum(LChMeasCQIDL13),sum(LChMeasCQIDL14),sum(LChMeasCQIDL15),sum(LChMeasCQIDL2),sum(LChMeasCQIDL3),sum(LChMeasCQIDL4),sum(LChMeasCQIDL5),sum(LChMeasCQIDL6),sum(LChMeasCQIDL7),sum(LChMeasCQIDL8),sum(LChMeasCQIDL9),sum(LChMeasPDSCHMCS0),sum(LChMeasPDSCHMCS1),sum(LChMeasPDSCHMCS10),sum(LChMeasPDSCHMCS11),sum(LChMeasPDSCHMCS12),sum(LChMeasPDSCHMCS13),sum(LChMeasPDSCHMCS14),sum(LChMeasPDSCHMCS15),sum(LChMeasPDSCHMCS16),sum(LChMeasPDSCHMCS17),sum(LChMeasPDSCHMCS18),sum(LChMeasPDSCHMCS19),sum(LChMeasPDSCHMCS2),sum(LChMeasPDSCHMCS20),sum(LChMeasPDSCHMCS21),sum(LChMeasPDSCHMCS22),sum(LChMeasPDSCHMCS23),sum(LChMeasPDSCHMCS24),sum(LChMeasPDSCHMCS25),sum(LChMeasPDSCHMCS26),sum(LChMeasPDSCHMCS27),sum(LChMeasPDSCHMCS28),sum(LChMeasPDSCHMCS29),sum(LChMeasPDSCHMCS3),sum(LChMeasPDSCHMCS30),sum(LChMeasPDSCHMCS31),sum(LChMeasPDSCHMCS4),sum(LChMeasPDSCHMCS5),sum(LChMeasPDSCHMCS6),sum(LChMeasPDSCHMCS7),sum(LChMeasPDSCHMCS8),sum(LChMeasPDSCHMCS9),sum(LChMeasPRBDLAvail),sum(LChMeasPRBDLUsedAvg),sum(LChMeasPRBPUSCHAvg),sum(LChMeasPRBULAvail),sum(LChMeasPRBULUsedAvg),sum(LCSFBE2G),sum(LCSFBE2W),sum(LCSFBPrepAtt),sum(LCSFBPrepFailConflict),sum(LCSFBPrepSucc),sum(LERABAbnormRel),sum(LERABAbnormRelCong),sum(LERABAbnormRelCongLoad),sum(LERABAbnormRelCongPreEmp),sum(LERABAbnormRelHOFailure),sum(LERABAbnormRelMME),sum(LERABAbnormRelMMEEUtranGen),sum(LERABAbnormRelRadio),sum(LERABAbnormRelRadioDRBReset),sum(LERABAbnormRelRadioSRBReset),sum(LERABAbnormRelRadioULSyncFail),sum(LERABAbnormRelRadioUuNoReply),sum(LERABAbnormRelTNL),sum(LERABAbnormRelTNLLoad),sum(LERABAbnormRelTNLPreEmp),sum(LERABAttEst),sum(LERABFailEstMME),sum(LERABFailEstNoRadioRes),sum(LERABFailEstNoReply),sum(LERABFailEstRNL),sum(LERABFailEstSecurModeFail),sum(LERABFailEstTNL),sum(LERABNormRel),sum(LERABSuccEst),sum(LHHOFailOutHOCancel),sum(LHHOIntereNBExecAttIn),sum(LHHOIntereNBExecSuccIn),sum(LHHOIntereNBIntraFreqExecAttOut),sum(LHHOIntereNBIntraFreqExecSuccOut),sum(LHHOIntereNBPathSwAtt),sum(LHHOIntereNBPathSwSucc),sum(LHHOIntraeNBExecAttIn),sum(LHHOIntraeNBExecSuccIn),sum(LHHOIntraeNBIntraFreqExecAttOut),sum(LHHOIntraeNBIntraFreqExecSuccOut),sum(LHHOPrepFailOutHOCancel),sum(LHHOPrepFailOutMME),sum(LHHOPrepFailOutNoReply),sum(LHHOPrepFailOutPrepFailure),sum(LPagingDisNum),sum(LPagingDisPchCong),sum(LPagingS1Rx),sum(LRADedicateAtt),sum(LRADedicatePreambleAssignNum),sum(LRADedicatePreambleReqNum),sum(LRAGrpAAtt),sum(LRAGrpBAtt),sum(LRATAUEIndex0),sum(LRATAUEIndex1),sum(LRATAUEIndex10),sum(LRATAUEIndex11),sum(LRATAUEIndex2),sum(LRATAUEIndex3),sum(LRATAUEIndex4),sum(LRATAUEIndex5),sum(LRATAUEIndex6),sum(LRATAUEIndex7),sum(LRATAUEIndex8),sum(LRATAUEIndex9),sum(LRAUeRaInfoRspNum),sum(LRAUeRaInfoRspWithConNum),sum(LRRCConnReqAtt),sum(LRRCConnReqAttDelayTol),sum(LRRCConnReqAttEmc),sum(LRRCConnReqAttHighPri),sum(LRRCConnReqAttMoData),sum(LRRCConnReqAttMt),sum(LRRCConnReqMsgdiscFlowCtrl),sum(LRRCConnReqSuccDelayTol),sum(LRRCConnReqSuccEmc),sum(LRRCConnReqSuccHighPri),sum(LRRCConnReqSuccMoData),sum(LRRCConnReqSuccMt),sum(LRRCRedirectionE2G),sum(LRRCRedirectionE2GCSFB),sum(LRRCRedirectionE2GPrepAtt),sum(LRRCRedirectionE2W),sum(LRRCRedirectionE2WCSFB),sum(LRRCRedirectionE2WPrepAtt),sum(LRRCSetupFailNoReply),sum(LRRCSetupFailRej),sum(LRRCSetupFailRejFlowCtrl),sum(LRRCSetupFailResFail),sum(LRRCSetupFailResFailPUCCH),sum(LRRCSetupFailResFailSRS),sum(LS1SigConnEstAtt),sum(LS1SigConnEstSucc),sum(LThrpbitsDL),sum(LThrpbitsDLLastTTI),sum(LThrpbitsDLPDCPPDU),sum(LThrpbitsUEULLastTTI),sum(LThrpbitsUL),sum(LThrpbitsULPDCPSDU),sum(LThrpTimeCellDL),sum(LThrpTimeCellDLHighPrecision),sum(LThrpTimeCellUL),sum(LThrpTimeCellULHighPrecision),sum(LThrpTimeDL),sum(LThrpTimeDLRmvLastTTI),sum(LThrpTimeUEULRmvLastTTI),sum(LThrpTimeUL),sum(LTrafficActiveUserAvg),sum(LTrafficActiveUserMax),sum(LTrafficUserAvg),sum(LTrafficUserDataAvg),sum(LTrafficUserDataMax),sum(LTrafficUserMax),sum(LTrafficUserUlsyncAvg),sum(LULInterferenceAvg),sum(LULInterferenceMax),sum(LULInterferenceMin),sum(LRRCConnReqSuccMoSig),sum(LRRCConnReqAttMoSig),sum(LHHOIntraeNBIntraFreqSuccReEst2Src),sum(LHHOIntereNBIntraFreqSuccReEst2Src),sum(LHHOX2IntraFreqExecAttOut),sum(LHHOX2IntraFreqExecSuccOut),sum(LHHOIntereNBX2TimeAvg),sum(LHHOIntereNBS1TimeAvg),sum(LHHOIntraeNBInterFreqExecSuccOut),sum(LHHOIntereNBInterFreqExecSuccOut),sum(LHHOIntraeNBInterFreqExecAttOut),sum(LHHOIntereNBInterFreqExecAttOut),sum(LTrafficDLSCHQPSKErrTBIbler),sum(LTrafficDLSCH16QAMErrTBIbler),sum(LTrafficDLSCH64QAMErrTBIbler),sum(LTrafficDLSCHQPSKTB),sum(LTrafficDLSCH16QAMTB),sum(LTrafficDLSCH64QAMTB),sum(LTrafficULSCHQPSKErrTBIbler),sum(LTrafficULSCH16QAMErrTBIbler),sum(LTrafficULSCH64QAMErrTBIbler),sum(LTrafficULSCHQPSKTB),sum(LTrafficULSCH16QAMTB),sum(LTrafficULSCH64QAMTB),sum(LThrpbitsDLCAUser),sum(LThrpbitsULCAUser),sum(LHHOIntraeNBIntraFreqPrepAttOut),sum(LHHOIntraeNBInterFreqPrepAttOut),sum(LHHOIntereNBIntraFreqPrepAttOut),sum(LHHOIntereNBInterFreqPrepAttOut),sum(LCAUEMax),sum(LCAUEAvg),sum(LCATrafficbitsDLPCell),sum(LCATrafficbitsDLSCell),sum(LTrafficUserPCellDLAvg),sum(LTrafficUserSCellDLAvg),sum(LTrafficULCEUAvg),sum(LTrafficULCEUMax),sum(LTrafficUserPCellDLMax),sum(LTrafficUserSCellDLMax),sum(LHHOIntereNBInterFddTddExecSuccOut),sum(LHHOIntraeNBInterFddTddExecAttOut),sum(LHHOIntereNBInterFddTddExecAttOut),sum(LHHOIntraeNBInterFddTddExecSuccOut),sum(LTrafficDLPktSizeSampIndex0),sum(LTrafficDLPktSizeSampIndex1),sum(LTrafficDLPktSizeSampIndex2),sum(LTrafficDLPktSizeSampIndex3),sum(LTrafficDLPktSizeSampIndex4),sum(LTrafficDLPktSizeSampIndex5),sum(LTrafficDLPktSizeSampIndex6),sum(LTrafficDLPktSizeSampIndex7),sum(LTrafficDLPktSizeSampIndex8),sum(LTrafficDLPktSizeSampIndex9),sum(LTrafficDLSCH256QAMTB),sum(LTrafficDLSCHQPSKTBbits),sum(LTrafficDLSCH16QAMTBbits),sum(LTrafficDLSCH64QAMTBbits),sum(LTrafficDLSCH256QAMTBbits) from counter_4g_lastday group by tanggal,unique_id,NENAME,ENODEBFUNCTIONNAME,LOCALCELLID,CELLNAME,ENODEBID,CELLFDDTDDINDICATION on conflict do nothing; truncate counter_4g_lastday;")
	
	fmt.Println("Success")
}

// report periodically prints the write rate in number of rows per second
func report() {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)

	for now := range time.NewTicker(reportingPeriod).C {
		rCount := atomic.LoadInt64(&rowCount)

		took := now.Sub(prevTime)
		rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
		overallRowrate := float64(rCount) / float64(now.Sub(start).Seconds())
		totalTook := now.Sub(start)

		fmt.Printf("at %v, row rate %f/sec (period), row rate %f/sec (overall), %E total rows\n", totalTook-(totalTook%time.Second), rowrate, overallRowrate, float64(rCount))

		prevRowCount = rCount
		prevTime = now
	}

}

// scan reads lines from a bufio.Scanner, each which should be in CSV format
// with a delimiter specified by --split (comma by default)
func scan(itemsPerBatch int, scanner *bufio.Scanner, batchChan chan *batch) int64 {
	rows := make([]string, 0, itemsPerBatch)
	var linesRead int64

	for scanner.Scan() {
		linesRead++

		rows = append(rows, scanner.Text())
		if len(rows) >= itemsPerBatch { // dispatch to COPY worker & reset
			batchChan <- &batch{rows}
			rows = make([]string, 0, itemsPerBatch)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if len(rows) > 0 {
		batchChan <- &batch{rows}
	}

	return linesRead
}

// processBatches reads batches from C and writes them to the target server, while tracking stats on the write.
func processBatches(wg *sync.WaitGroup, C chan *batch) {

	columnCountWorker := int64(0)
	for batch := range C {
		start := time.Now()

		tx := dbBench.MustBegin()
		delimStr := fmt.Sprintf("'%s'", splitCharacter)
		if splitCharacter == "\\t" {
			delimStr = "E" + delimStr
		}
		var copyCmd string
		if columns != "" {
			copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s", getFullTableName(), columns, delimStr, copyOptions)
		} else {
			copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s", getFullTableName(), delimStr, copyOptions)
		}
		
		//fmt.Println(copyCmd)

		stmt, err := tx.Prepare(copyCmd)
		if err != nil {
			panic(err)
		}

		// Need to cover the string-ified version of the character to actual character for correct split
		sChar := splitCharacter
		if sChar == "\\t" {
			sChar = "\t"
		}
		
		//fmt.Println(sChar)
		//os.Exit(3)
		
		for _, line := range batch.rows {
			sp := strings.Split(line, sChar)

			var unique_id = sp[5] + sp[3]
			slice_1 := make([]string, 2)
			slice_1[0] = sp[0]
			slice_1[1] = unique_id

			var slice_2 []string = sp[1:]
			new_sp := append(slice_1, slice_2...)
			
			finalCommand := strings.Join(new_sp, ",")
			
			if _, err := f.WriteString(finalCommand + "\n+++++++++++++++++++++++++++++++++++++++++++++++++\n"); err != nil {
				log.Println(err)
			}

			columnCountWorker += int64(len(new_sp))
			// For some reason this is only needed for tab splitting
			if sChar == "\t" {
				args := make([]interface{}, len(new_sp))
				for i, v := range new_sp {
					args[i] = v
				}
				_, err = stmt.Exec(args...)
			} else {
				//fmt.Println("Masuk else")
				_, err = stmt.Exec(finalCommand)
			}

			if err != nil {
				panic(err)
			}
		}
		atomic.AddInt64(&columnCount, columnCountWorker)
		atomic.AddInt64(&rowCount, int64(len(batch.rows)))
		columnCountWorker = 0

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}

		if logBatches {
			took := time.Now().Sub(start)
			fmt.Printf("[BATCH] took %v, batch size %d, row rate %f/sec\n", took, batchSize, float64(batchSize)/float64(took.Seconds()))
		}

	}
	wg.Done()
}
