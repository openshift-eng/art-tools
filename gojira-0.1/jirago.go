package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/andygrunwald/go-jira"
	"github.com/davecgh/go-spew/spew"
)

func main() {

	basePtr := flag.String("ep", "https://projects.engineering.redhat.com", "jira endpoint url")
	userPtr := flag.String("user", "openshift-art-automation", "jira username")
	passwdPtr := flag.String("password", "", "jira password")
	projectPtr := flag.String("project", "CLOUDDST", "jira project")
	typePtr := flag.String("type", "Task", "jira issue type")
	sumPtr := flag.String("summary", "OCP Tarball sources Test", "jira summary")
	descPtr := flag.String("description", "", "jira description")

	flag.Parse()

	fmt.Println("endpoint:", *basePtr)
	fmt.Println("username:", *userPtr)
	fmt.Println("password:", *passwdPtr)
	fmt.Println("project:", *projectPtr)
	fmt.Println("type:", *typePtr)
	fmt.Println("summary:", *sumPtr)
	fmt.Println("description:", *descPtr)


	tp := jira.BasicAuthTransport{
		Username: *userPtr,
		Password: *passwdPtr,
	}

	jiraClient, err := jira.NewClient(tp.Client(), *basePtr)
	if err != nil {
		panic(err)
	}

	i := jira.Issue{
		Fields: &jira.IssueFields{
			//Assignee: &jira.User{
			//	Name: "myuser",
			//},
			Reporter: &jira.User{
				Name: *userPtr,
			},
			Description: *descPtr,
			Type: jira.IssueType{
				Name: *typePtr,
			},
			Project: jira.Project{
				Key: *projectPtr,
			},
			Summary: *sumPtr,
		},
	}
	issue, resp, err := jiraClient.Issue.Create(&i)
	if err != nil {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		newStr := buf.String()
		fmt.Println(newStr)
		panic(err)
	}
	spew.Dump(issue)
}
