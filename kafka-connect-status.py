import os
import requests
import time

cert_loc = os.getenv("CA_CERT_LOC")
kafka_connect = os.getenv("KAFKA_CONNECT_URL")
slack_url = os.getenv("SLACK_WEBHOOK_URL")
env = os.getenv("ENV_NAME")
kafka_connect_url = kafka_connect + "/connectors"


def restartConnector(connector_name):
    print("Restarting kafka connector:" +connector_name)
    connector_restart_post_url = kafka_connect_url + "/" + connector_name + "/restart"
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.post(connector_restart_post_url, headers=headers, verify=cert_loc)

    if response.status_code == 204:
        print("Kafka connector:" + connector_name + " restarted successfully, status code:" + str(
            response.status_code))
        time.sleep(2)
        print("Checking connector status.....")
        connector_status_url = kafka_connect_url + "/" + connector_name + "/status"
        status_response = requests.get(connector_status_url, headers=headers, verify=cert_loc)
        resp = status_response.json()
        conStatus = resp['connector']['state']
        sendSlackMessage(connector_name, conStatus, None, False, True, None, None)
    elif response.status_code == 405:
        print("Kafka connector:" + connector_name + " method not allowed, status code:" + str(response.status_code))
    else:
        print("Failed to restart kafka connector:" + connector_name + ", status code:" + str(response.status_code))

def restartTasks(connector_name, task_id):
    print("Restarting kafka connector tasks for " +connector_name + "Task Id:"+str(task_id))

    connector_task_restart_post_url = kafka_connect_url + "/" + connector_name + "/tasks/" + str(task_id) + "/restart"
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.post(connector_task_restart_post_url, headers=headers, verify=cert_loc)
    if response.status_code == 204:
        print("Kafka connector task:" + str(task_id) + " restarted successfully, status code:" + str(
            response.status_code))
        time.sleep(2)
        print("Checking task status.....")
        connector_status_url = kafka_connect_url + "/" + connector_name + "/status"
        connector_task_status_url = kafka_connect_url + "/" + connector_name + "/tasks/" + str(task_id) + "/status"
        con_status_response = requests.get(connector_status_url, headers=headers, verify=cert_loc)
        task_status_response = requests.get(connector_task_status_url, headers=headers, verify=cert_loc)
        con_resp = con_status_response.json()
        task_resp = task_status_response.json()
        conStatus = con_resp['connector']['state']
        taskStatus = task_resp['state']
        sendSlackMessage(connector_name, conStatus, taskStatus, True, True, task_id, None)
    elif response.status_code == 405:
        print("Kafka connector task:" + str(task_id) + " method not allowed, status code:" + str(response.status_code))
    else:
        print("Failed to restart kafka connector task:" + str(task_id) + ", status code:" + str(response.status_code))

def sendSlackMessage(connect, conStatus, taskStatus, isTask, isRestart, task_id, trace):
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    if isRestart is True:
        if isTask is True:
            data = '{\"username\": "kafka-connect-task-restart-' + env + '\", \"text\":\"Connector:' + connect + ', Connector Status:' + conStatus + ', Task Id:' + str(task_id) + ', Task Status:' + taskStatus +'\"}'
            slack_resp = requests.post(slack_url, headers=headers, verify=False, data=data)
            print("Slack API response:" + str(slack_resp))
        else:
            data = '{\"username\": "kafka-connect-restart-' + env + '\", \"text\":\"Connector:' + connect + ', Connector Status:' + conStatus + '\"}'
            slack_resp = requests.post(slack_url, headers=headers, verify=False, data=data)
            print("Slack API response:" + str(slack_resp))
    if isRestart is False:
        if isTask is True:
            if trace is not None:
                data = '{\"username\": "kafka-connect-task-' + env + '\", \"text\":\"Connector:' + connect + ', Connector Status:' + conStatus + ', Task Id:' + str(task_id) + ', Task Status:' + taskStatus + ', Trace:' + str(trace) +'\"}'
                slack_resp = requests.post(slack_url, headers=headers, verify=False, data=data)
                print("Slack API response:" + str(slack_resp))
            else:
                data = '{\"username\": "kafka-connect-task-' + env + '\", \"text\":\"Connector:' + connect + ', Connector Status:' + conStatus + ', Task Id:' + str(task_id) + ', Task Status:' + taskStatus +'\"}'
                slack_resp = requests.post(slack_url, headers=headers, verify=False, data=data)
                print("Slack API response:" + str(slack_resp))
        else:
            data = '{\"username\": "kafka-connect-' + env + '\", \"text\":\"Connector:' + connect + ', Connector Status:' + conStatus + '\"}'
            slack_resp = requests.post(slack_url, headers=headers, verify=False, data=data)
            print("Slack API response:" + str(slack_resp))

def heakthCheck():
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.get(kafka_connect_url, headers=headers, verify=cert_loc)
    print("Connectors:" + str(response.json()))

    for connect in response.json():
        print("connector:" + connect)
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        response = requests.get(kafka_connect_url + '/' + connect + '/status', headers=headers, verify=cert_loc)
        resp = response.json()
        conStatus = resp['connector']['state']
        tasks = resp['tasks']
        print("Tasks:" + str(tasks))
        print("Tasks size:" + str(len(tasks)))
        print('Connector state:'+ conStatus)
        if conStatus != 'RUNNING':
            sendSlackMessage(connect, conStatus, None, False, False, None, None)
            restartConnector(connect)
        task_id = 0
        while task_id < len(tasks):
            response = requests.get(kafka_connect_url + '/' + connect + '/tasks/'+str(task_id)+'/status', headers=headers, verify=cert_loc)
            resp = response.json()
            taskStatus = resp['state']
            print('Task state:' + taskStatus)
            if taskStatus != 'RUNNING':
                if taskStatus == 'FAILED':
                    trace = resp['trace']
                    trace = trace.replace("\n", " ")
                    trace = trace.replace("\"", "\\\"")
                    sendSlackMessage(connect, conStatus, taskStatus, True, False, task_id, trace)
                else:
                    sendSlackMessage(connect, conStatus, taskStatus, True, False, task_id, None)
                restartTasks(connect, task_id)
            task_id += 1

def main():
    heakthCheck()

if __name__ == '__main__':
    main()
