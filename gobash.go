package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sstask/golib/stconfig"
	"github.com/sstask/golib/stmysql"
)

type t_server struct {
	App       string
	Server    string
	Division  string
	Node      string
	Status    int
	Use_agent int
}

type t_service struct {
	App      string
	Server   string
	Division string
	Node     string
	Service  string
	Endpoint string
}

var ConnID map[int]*t_server
var GameID map[int]*t_server
var BattleID map[int]*t_server
var MatchID map[int]*t_server
var FriendID map[int]*t_server
var DirtyID map[int]*t_server
var AllServer map[string][]*t_server

var ConnService map[int]*t_service
var BattService map[int]*t_service

var wg sync.WaitGroup

func release(num string, svrpack string, isRs bool) {
	divstr := "all"
	if strings.Contains(svrpack, "Game") || strings.Contains(svrpack, "Conn") {
		divstr = "moba.zone."
	} else if strings.Contains(svrpack, "Battle") {
		divstr = "moba.battle."
	} else if strings.Contains(svrpack, "Match") {
		divstr = "moba.match."
	} else if strings.Contains(svrpack, "Friend") {
		divstr = "moba.friend."
	} else if strings.Contains(svrpack, "Dirty") {
		divstr = "moba.dirty."
	}

	if num == "" {
		divstr = "all"
	}

	strcmd := `./cc_release_server.sh ` + svrpack + " " + divstr
	if divstr != "all" {
		strcmd = strcmd + num
	}
	fmt.Println("exe cmd=", strcmd)
	if !isRs {
		wg.Done()
		return
	}
	cmd := exec.Command("/bin/sh", "-c", strcmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(out))
	wg.Done()
	fmt.Println("----------------------------Success ", num)
	fmt.Println("")
}

func releaseServer(args []string, isRs bool) {
	arnum := len(args)
	if arnum == 0 {
		fmt.Println("you need input the server name")
		return
	}
	strcmd := `ls -l -t /data/version_backup|tac|grep ` + args[0] + `|tail --lines=1|awk '{print $9}'`
	cmd := exec.Command("/bin/sh", "-c", strcmd)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)
		return
	}
	svrpack := string(out)
	if svrpack == "" {
		fmt.Println("can not find ", args[0])
		return
	}
	svrpack = strings.Replace(svrpack, "\n", "", -1)
	svrpack = strings.Replace(svrpack, "\r", "", -1)
	fmt.Println("find server version=", svrpack)

	var rlMap map[int]*t_server
	if strings.Contains(svrpack, "Conn") {
		rlMap = ConnID
	} else if strings.Contains(svrpack, "Game") {
		rlMap = GameID
	} else if strings.Contains(svrpack, "Battle") {
		rlMap = BattleID
	} else if strings.Contains(svrpack, "Match") {
		rlMap = MatchID
	} else if strings.Contains(svrpack, "Friend") {
		rlMap = FriendID
	} else if strings.Contains(svrpack, "Dirty") {
		rlMap = DirtyID
	}

	if arnum == 1 { //only serername
		wg.Add(1)
		go release("", svrpack, isRs)
	} else if arnum == 2 {
		divID, _ := strconv.Atoi(args[1])

		for k, _ := range rlMap {
			if k == divID || k/100 == divID {
				wg.Add(1)
				go release(strconv.Itoa(k), svrpack, isRs)
			}
		}
	} else if arnum == 3 {
		begin, _ := strconv.Atoi(args[1])
		end, _ := strconv.Atoi(args[2])
		if begin != end {
			for ; begin <= end; begin++ {
				wg.Add(1)
				go release(strconv.Itoa(begin), svrpack, isRs)
			}
		} else {
			for k, _ := range rlMap {
				if k == begin || k/1000 == begin {
					wg.Add(1)
					go release(strconv.Itoa(k), svrpack, isRs)
				}
			}
		}
	}
	wg.Wait()
	//fmt.Println("+++++++++++++++++++++++++++++Success")
}

func readMfw() {
	node, err := stconfig.LoadXml("cfg.xml")
	if err != nil {
		panic(err.Error())
	}
	sqlIp := node.FindNode("DBUser").GetVal() + ":" + node.FindNode("DBPWD").GetVal() + "@tcp(" + node.FindNode("DBIP").GetVal() + ")/db_mfw"
	db, err := sql.Open("mysql", sqlIp)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	cols, err := stmysql.SelectAll(db, t_server{})
	if err != nil {
		panic(err.Error())
	}

	ConnID = make(map[int]*t_server)
	GameID = make(map[int]*t_server)
	BattleID = make(map[int]*t_server)
	MatchID = make(map[int]*t_server)
	FriendID = make(map[int]*t_server)
	DirtyID = make(map[int]*t_server)
	AllServer = make(map[string][]*t_server)

	for _, v := range cols {
		svr := v.(*t_server)
		AllServer[svr.Server] = append(AllServer[svr.Server], svr)

		div := strings.Split(svr.Division, ".")
		if len(div) != 3 {
			continue
		}
		svrid, _ := strconv.Atoi(div[2])
		switch svr.Server {
		case "ConnServer":
			ConnID[svrid] = svr
		case "GameServer":
			GameID[svrid] = svr
		case "BattleServer":
			BattleID[svrid] = svr
		case "MatchServer":
			MatchID[svrid] = svr
		case "FriendServer":
			FriendID[svrid] = svr
		case "DirtyCheckServer":
			DirtyID[svrid] = svr
		}
	}

	/*for k, v := range GameID {
		fmt.Println(k, v)
	}*/

	ConnService = make(map[int]*t_service)
	BattService = make(map[int]*t_service)

	cols, err = stmysql.SelectAll(db, t_service{})
	if err != nil {
		panic(err.Error())
	}
	for _, v := range cols {
		//fmt.Println(v.(*t_service))
		svr := v.(*t_service)
		div := strings.Split(svr.Division, ".")
		if len(div) != 3 {
			continue
		}
		svrid, _ := strconv.Atoi(div[2])
		if svr.Server == "BattleServer" && svr.Service == "HandleConn" {
			BattService[svrid] = svr
		}
		if svr.Server == "ConnServer" && svr.Service == "HandleConn" {
			ConnService[svrid] = svr
		}
	}
}

func addBattle(id int, outip string, outport int, inip string, inport int) {
	node, err := stconfig.LoadXml("cfg.xml")
	if err != nil {
		panic(err.Error())
	}
	sqlIp := node.FindNode("DBUser").GetVal() + ":" + node.FindNode("DBPWD").GetVal() + "@tcp(" + node.FindNode("DBIP").GetVal() + ")/db_mfw"
	db, err := sql.Open("mysql", sqlIp)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	batdivision := "moba.battle." + strconv.Itoa(id)
	batserver := &t_server{App: "MOBA", Server: "BattleServer", Division: batdivision, Node: inip, Status: 1, Use_agent: 1}
	batendpointIn := "tcp -h " + inip + " -p " + strconv.Itoa(inport) + " -t 60000"
	batserviceIn := &t_service{App: "MOBA", Server: "BattleServer", Division: batdivision, Node: inip, Service: "BattleServiceObj", Endpoint: batendpointIn}
	batendpointOut := "tcp -h " + outip + " -p " + strconv.Itoa(outport) + " -t 60000"
	batserviceOut := &t_service{App: "MOBA", Server: "BattleServer", Division: batdivision, Node: inip, Service: "HandleConn", Endpoint: batendpointOut}

	_, err = stmysql.InsertOne(db, batserver)
	if err != nil {
		panic(err.Error())
	}
	_, err = stmysql.InsertOne(db, batserviceIn)
	if err != nil {
		panic(err.Error())
	}
	_, err = stmysql.InsertOne(db, batserviceOut)
	if err != nil {
		panic(err.Error())
	}
}

func getIP(id int) (outip string, inip string) {
	svr, ok := ConnService[id]
	if !ok {
		svr, ok = BattService[id]
		if !ok {
			fmt.Println("can not find this server id=", id)
			return "", ""
		}
	}

	inip = svr.Node

	endPoint := strings.Split(svr.Endpoint, " ")
	if len(endPoint) > 3 {
		outip = endPoint[2]
	}
	return outip, inip
}

func main() {
	readMfw()
	//addBattle(22005, "114.215.193.156", 21114, "10.161.223.103", 21113)
	//fmt.Println(getIP(22008))

	if len(os.Args) < 3 {
		fmt.Println(`gobash ch|rs PackNmae Div|Num1 Num2
gobash ab BS_ID BS_OUTIP BS_OUTPORT BS_INIP BS_INPORT (NUM)
gobash ip BattleID|ConnID`)
		return
	}

	if os.Args[1] == "ch" {
		releaseServer(os.Args[2:], false)
	} else if os.Args[1] == "rs" {
		releaseServer(os.Args[2:], true)
	} else if os.Args[1] == "ab" {
		if len(os.Args) < 7 {
			fmt.Println("ab need 5 params at least")
			return
		}
		id, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err.Error())
		}
		outport, err := strconv.Atoi(os.Args[4])
		if err != nil {
			panic(err.Error())
		}
		inport, err := strconv.Atoi(os.Args[6])
		if err != nil {
			panic(err.Error())
		}
		abnum := 1
		if len(os.Args) > 7 {
			abnum, err = strconv.Atoi(os.Args[7])
			if err != nil {
				panic(err.Error())
			}
		}
		for i := 0; i < abnum; i++ {
			addBattle(id+i, os.Args[3], outport+i, os.Args[5], inport+i)
		}
	} else if os.Args[1] == "ip" {
		if len(os.Args) < 2 {
			fmt.Println("ip need 1 params at least")
			return
		}
		id, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err.Error())
		}
		fmt.Println(getIP(id))
	}
}
