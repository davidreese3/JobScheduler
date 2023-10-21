/* program: Batch Job Dispatcher - Processes
   Author:  David Reese
   File:    JobScheduler.c
   Compile: gcc -o JS JobScheduler.c
   Run:     ./JS

   This program has two main threads of a scheduler and a dispatcher.
   The scheduler will take jobs in from the user and schedule 
   them when they should execute. At the same time the dispatcher,
   will dispact the jobs via an executer thread. 
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <pthread.h>

struct JOB {
   char *command[5];     
   long submitTime;      // domain = { positive long integer, seconds }
   long startTime;       // domain = { positive long integer, seconds }
   struct JOB *next; 
};
typedef struct JOB Job;

struct LIST{
   int numOfJobs;        // number of jobs in the list
   Job *firstJob;        // points to first job. NULL if empty
   Job *lastJob;         // points to last job. NULL if empty
};
typedef struct LIST List;

void printJob(Job *jobPtr){
   printf("command: ");
   for(int i=0; jobPtr->command[i] != NULL; i++){
      printf("%s ", jobPtr->command[i]);
   }
   printf("\n");
   printf("submission time: %ld\n",jobPtr->submitTime);
   printf("start time: %ld\n",jobPtr->startTime);
}

void printJobs(List *list){
   if(list->numOfJobs != 0){
      Job *jobPtr = list->firstJob;
      for (int i=0; i < list->numOfJobs; i++){
         printf("Job %d:\n", i+1);        
         printJob(jobPtr);
         jobPtr = jobPtr->next;
      }
   } else {
      printf("List Empty\n");
   }
}

List* createList(){
   List* list = (List*) malloc(sizeof(List));
   list->firstJob = NULL;
   list->lastJob = NULL;
   list->numOfJobs = 0;
   return list;
}

Job* createJob(){
   Job* jobPtr = (Job*) malloc(sizeof(Job));
   int numOfCommands;
   scanf("%d",&(numOfCommands)); 
   int i;
   for(i=0; i < numOfCommands && i < 5; i++){
      jobPtr->command[i] = (char*) malloc(25*sizeof(char));
      scanf("%s", jobPtr->command[i]);
   }
   jobPtr->command[i] = NULL;
   scanf("%ld", &(jobPtr->startTime));
   jobPtr->submitTime = time(NULL);
   jobPtr->next = NULL;
   return jobPtr;
}

long getExecTime(Job *jobPtr){
   return (jobPtr->submitTime) + (jobPtr->startTime);
}

Job* submitTimes(Job *jobPtr, Job *currPos, Job *prevPos){
   long jobSubmitTime = jobPtr->submitTime;
   long currSubmitTime = currPos->submitTime;
   long totalTime = getExecTime(jobPtr);
   long currTime = getExecTime(currPos);
   while(currSubmitTime < jobSubmitTime){
      prevPos = currPos;
      currPos = currPos->next;
      currTime = getExecTime(currPos);   
      if(currTime > totalTime){
         break;
      } else {
         currSubmitTime = currPos->submitTime;
      }
   }
   return prevPos;
}

void insertBetweenJobs(List *list, Job *jobPtr){
   Job* prevPos;
   Job* currPos = list->firstJob;
   Job* tmp;
   long totalTime = getExecTime(jobPtr);
   long currTime = getExecTime(currPos);
   while(currTime < totalTime){
      prevPos = currPos;
      currPos = currPos->next;
      currTime = getExecTime(currPos);
   }
   if(currTime == totalTime){
      tmp =  submitTimes(jobPtr, currPos, prevPos);
      jobPtr->next = tmp->next;
      tmp->next = jobPtr;
   } else {
      jobPtr->next =  prevPos->next;
      prevPos->next = jobPtr;
   }
}

void appendJob(List *list, Job *jobPtr){
   if(list->firstJob != NULL){
      list->lastJob->next = jobPtr;
      list->lastJob = jobPtr;
   } else{
      list->lastJob = jobPtr;
      list->firstJob = jobPtr;
   }
   jobPtr->next = NULL;
   list->numOfJobs = (list->numOfJobs)+1;
}

void insertIntoEmpty(List *list, Job *jobPtr){
   appendJob(list, jobPtr);
}

void insertFirstJob(List *list, Job *jobPtr){
   jobPtr->next = list->firstJob;
   list->firstJob = jobPtr;
}

void insertLastJob(List *list, Job *jobPtr){
   list->lastJob->next = jobPtr;
   list->lastJob = jobPtr;
   jobPtr->next = NULL;
}

void insertJob(List *list, Job *jobPtr){
   if(list->numOfJobs == 0){
      insertIntoEmpty(list, jobPtr);
   } else {
      long totalTime = getExecTime(jobPtr);
      long firstTime = getExecTime(list->firstJob);
      long lastTime = getExecTime(list->lastJob);
      if(totalTime < firstTime){
         insertFirstJob(list, jobPtr);
      }
      else if(totalTime >= lastTime){
         insertLastJob(list, jobPtr);
      } else {
         insertBetweenJobs(list, jobPtr);
      }
      list->numOfJobs = (list->numOfJobs)+1;
   }
}

Job *deleteFirstJob(List *list){
   if(list->numOfJobs != 0){
      Job *tmp = list->firstJob;
      list->firstJob = list->firstJob->next;
      list->numOfJobs = (list->numOfJobs) - 1;
      return tmp;
   } else {
      return NULL;
   }
}

void freeCharArray(char **command){
   for(int i=0; command[i] == NULL; i++){
      free(command[i]);
   }
}

void deletePrint(List *list){
   if(list->numOfJobs == 0){
      printf("No Job Deleted\n");
   } else {
      printf("Job Deleted:\n");
      printJob(list->firstJob);
   }
}

void freeJob(Job *job){
   freeCharArray(job->command);
   free(job);
}

void *execute(void *ptr){
   Job *job = (Job*) ptr;
   int child_pid, status = 999;
   if((child_pid = fork()) != 0){
      waitpid(child_pid, &status, WEXITED);
   } else {
      execvp(job->command[0], job->command);
   }
   printJob(job);
   printf("execution Status: %d\n", WEXITSTATUS(status));
   freeJob(job);
   pthread_exit(NULL);
}

void *dispatcher(void *ptr){
   List *list = (List*) ptr;
   long currentSysTime;
   while(1){
      if(list->numOfJobs > 0){
         currentSysTime = time(NULL);
         if(currentSysTime >= getExecTime(list->firstJob)){
            printf("\nCurrent System Time: %ld\n", currentSysTime);
            Job* jobToExecute = deleteFirstJob(list);
            pthread_t executerThread;
            pthread_create(&executerThread, NULL, execute, jobToExecute);
         } 
         else {
            sleep(1);
         }
      }
   }
}

void *scheduler(void * ptr){
   List *list = (List*) ptr;
   char input;
   while(1){
      scanf("%c", &(input));
      if(input == '+'){
         Job* jobPtr = createJob();
         insertJob(list, jobPtr); 
      }
      if(input == 'p'){
          printJobs(list);
      }
      if(input == '-'){
         deletePrint(list);
         if(list->numOfJobs >0){
            freeJob(deleteFirstJob(list));
         }
      }
   }
}

int main(){
   List *list = createList();
   pthread_t schedulerThread, dispatcherThread;
   pthread_create(&schedulerThread, NULL, scheduler, list);
   pthread_create(&dispatcherThread, NULL, dispatcher, list);
   pthread_join(schedulerThread, NULL);
   pthread_join(dispatcherThread, NULL);
}