#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
//================================ thread pool header ============================
#define SEM_WAIT(ptr)         \
    if (sem_wait((ptr)) != 0) \
    {                         \
        perror("sem_wait");   \
        exit(2);              \
    }
#define SEM_INIT(ptr, val)              \
    if (sem_init((ptr), 0, (val)) != 0) \
    {                                   \
        perror("sem_init");             \
        exit(1);                        \
    }
#define SEM_POST(ptr)         \
    if (sem_post((ptr)) != 0) \
    {                         \
        perror("sem_post");   \
        exit(3);              \
    }
#define SEM_DESTROY(sem_ptr)        \
    if (sem_destroy(sem_ptr) == -1) \
    {                               \
        perror("sem_destroy");      \
        exit(7);                    \
    }
// *************************************** define a task **************************************
typedef void *task_func_t(void *arg);
typedef struct _task_t
{
    task_func_t *func;
    void *arg;
    struct _task_t *next;
} task_t;
// return task_p
task_t *task_set(task_t *task_p, task_func_t *func, void *arg)
{
    task_p->func = func;
    task_p->arg = arg;
    task_p->next = NULL;
    return task_p;
}

// *********************************** define a task queue **********************************
typedef struct
{
    // head points to the first task of taskQ
    // tail points to the last task of taskQ
    task_t *head, *tail;
    sem_t unallocated_sem;
    sem_t _sem;
} taskQ_t;

void task_enqueue(taskQ_t *taskQ_p, task_func_t *func, void *arg);

// init task queue
void taskQ_init(taskQ_t *taskQ_p)
{
    taskQ_p->head = taskQ_p->tail = NULL;
    // simulate a binary semaphore
    SEM_INIT(&(taskQ_p->_sem), 1);
    SEM_INIT(&(taskQ_p->unallocated_sem), 0);
}

void taskQ_destroy(taskQ_t *taskqp) {
    SEM_DESTROY(&(taskqp->_sem));
    SEM_DESTROY(&(taskqp->unallocated_sem));
}

void task_enqueue(taskQ_t *taskQ_p, task_func_t *func, void *arg)
{
    SEM_WAIT(&(taskQ_p->_sem));

    task_t *tmp_p = (task_t *)malloc(sizeof(task_t));
    task_set(tmp_p, func, arg);

    // if task queue is empty
    if (taskQ_p->head == taskQ_p->tail && taskQ_p->tail == NULL)
    {
        taskQ_p->head = taskQ_p->tail = tmp_p;
        tmp_p = NULL;
        SEM_POST(&(taskQ_p->unallocated_sem));
        SEM_POST(&(taskQ_p->_sem));
        return;
    }
    // otherwise, insert new task to next of *tail
    taskQ_p->tail->next = tmp_p;
    taskQ_p->tail = tmp_p;

    SEM_POST(&(taskQ_p->unallocated_sem));
    SEM_POST(&(taskQ_p->_sem));
}

task_t *task_dequeue(taskQ_t *taskQ_p)
{
    SEM_WAIT(&(taskQ_p->_sem));

    // pop a job
    task_t *tmp_p = taskQ_p->head;

    // if last task is pop out, clear tail to NULL
    if (taskQ_p->head == taskQ_p->tail)
        taskQ_p->tail = NULL;
    // update head
    taskQ_p->head = taskQ_p->head->next;

    tmp_p->next = NULL;

    SEM_POST(&(taskQ_p->_sem));
    return tmp_p;
}

// *********************** worker fetch from taskq directly ********************
typedef struct
{
    size_t thread_cnt;
    taskQ_t *taskq;
    pthread_t *workers;
} tpool_t;

void tpool_init(tpool_t *pp, size_t thread_cnt)
{
    pp->thread_cnt = thread_cnt;
    pp->taskq = (taskQ_t *)malloc(sizeof(taskQ_t));
    taskQ_init(pp->taskq);
    pp->workers = (pthread_t *)malloc(thread_cnt * sizeof(pthread_t));
}

void tpool_destroy(tpool_t *pp) {
    free(pp->workers);
    taskQ_destroy(pp->taskq);
    free(pp->taskq);
}

void *worker(void *arg);

void pool_run(tpool_t *pp)
{
    size_t thread_cnt = pp->thread_cnt;
    for (size_t i = 0; i < thread_cnt; i++)
    {
        pthread_create(pp->workers + i, NULL, &worker, pp->taskq);
    }

    for (size_t i = 0; i < thread_cnt; i++)
    {
        if (pthread_join(pp->workers[i], NULL) != 0)
        {
            perror("pthread_join");
            exit(5);
        }
    }
    task_t *term_task = task_dequeue(pp->taskq);
    assert(term_task->func == NULL);
    free(term_task);
}

void *worker(void *arg)
{
    taskQ_t *taskq = (taskQ_t *)arg;
    task_t *taskp;

    for (;;)
    {
        SEM_WAIT(&(taskq->unallocated_sem));
        taskp = task_dequeue(taskq);
        if (taskp->func == NULL)
        {
            task_enqueue(taskq, taskp->func, taskp->arg); // trigger other workers to return
            free(taskp);
            return 0;
        }
        *(taskp->func)(taskp->arg);
        free(taskp);
        taskp = NULL;
    }
}
//============================= end thread pool header ===========================
#define MAX_THREAD 8
#define MAX_SIZE 1003520
#define HEIGHT 4
size_t input_size;
int input[MAX_SIZE];
int temp[MAX_SIZE];
int result[MAX_SIZE];

static inline double tv_diffms(struct timeval *start, struct timeval *end) {
  return (double)((end->tv_sec - start->tv_sec) * 1000000 + (end->tv_usec - start->tv_usec)) / 1000;
}

/*
 Each child share common parent to its sibling.
 Once a child complete, wait semaphore in corresponding position which is initiallized to 1.
 Use sem_trywait to check whether another sibling complete.
*/
//parent count = last layer count -1 = 2**height -1
sem_t one_child_finish[1 << HEIGHT];
taskQ_t *tq;

typedef struct {
  unsigned l, r;
} arr_range_t;
//store all index info, aware that this should keep readonly once it is initialized.
arr_range_t arr_ranges[2 << HEIGHT];

void *adaptive_sort(void *arg);

void read_input() {
  FILE *f = fopen("./input.txt", "r");
  if (f == NULL) {
    perror("fopen");
    exit(1);
  }
  fscanf(f, "%lu \n", &input_size);
  for (size_t i = 0; i < input_size; i++) {
    fscanf(f, "%d", &input[i]);
  }
  fclose(f);
}

/*
 * function should be accessable to a taskq(shared, use global variable),
 * parent_idx and its corresponding range
*/
void check_children_submit_task(size_t parent_idx) {
  if (parent_idx == 0) {
    //printf("terminate function enqueue %d\n", parent_idx);
    task_enqueue(tq, NULL, NULL);
  }
  if (sem_trywait(&one_child_finish[parent_idx]) != 0) {
    if (errno != EAGAIN) {
      perror("sem_trywait");
      exit(6);
    }
    task_enqueue(tq, adaptive_sort, parent_idx);
    return;
  }
  //else, first complete
}

//do merge
void merge_sort(size_t cur_idx) {
  //when this function is called, must ensure two children have finished.
  const size_t l = arr_ranges[cur_idx].l;
  const size_t r = arr_ranges[cur_idx].r;
  //copy two part to temp array
  const size_t cnt = (r-l+1);
  memcpy(temp+l, result+l, cnt * sizeof(int));
  //left last
  const size_t mid = l + ((r-l) >> 1);
  //two pointer
  size_t i = l, j = mid+1;
  size_t k = l;
  while (k <= r) {
    if (i <= mid && j <= r) {
      result[k++] = (temp[i] < temp[j]) ? temp[i++] : temp[j++];
    } else if (i <= mid) {
      result[k++] = temp[i++];
    } else {//if (j <= r)
      result[k++] = temp[j++];
    }
  }
}

void bubble_sort(size_t cur_idx) {
  size_t l = arr_ranges[cur_idx].l;
  size_t r = arr_ranges[cur_idx].r;
  for (size_t i = l; i <= r; i++) {
    for (size_t j = r; j > i; j--) {
      if (result[j] < result[j-1]) {
        int tmp = result[j];
        result[j] = result[j-1];
        result[j-1] = tmp;
      }
    }
  }
}

//router function for choosing sort algorithm
void *adaptive_sort(void *arg) {
  unsigned cur_idx = (unsigned)arg;
  unsigned parent_idx = cur_idx >> 1;

  if (cur_idx < (1 << (HEIGHT-1))) {
    //printf("sorting %d segment %d to %d by merge sort\n", cur_idx, arr_ranges[cur_idx].l, arr_ranges[cur_idx].r);
    merge_sort(cur_idx);
  } else {
    //printf("sorting %d segment %d to %d by bubble sort\n", cur_idx, arr_ranges[cur_idx].l, arr_ranges[cur_idx].r);
    bubble_sort(cur_idx);
  }
  
  check_children_submit_task(parent_idx);

  return (void *)0;
}

void do_sort(tpool_t *pp, size_t t_cnt) {
  tpool_init(pp, t_cnt);
  tq = pp->taskq;
  
  //submit leave node to taskq
  for (size_t i = (1 << (HEIGHT-1)); i < (1 << HEIGHT); i++) {
    task_enqueue(tq, adaptive_sort, i);
  }
  pool_run(pp);
  tpool_destroy(pp);
}


void build_range_arr(size_t idx, size_t l, size_t r) {
  //check out of bound
  if (idx >= (2 << HEIGHT))
    return;
  arr_ranges[idx].l = l;
  arr_ranges[idx].r = r;
  if (l == r) return;
  size_t mid = l + ((r-l) >> 1);
  build_range_arr((idx << 1), l, mid);
  build_range_arr((idx << 1) + 1, mid+1, r);
}

void check_ans(int l, int r, int cur_idx) {
  int res = 1;
  for (size_t i = l; i < r; i++)
    if (result[i] > result[i+1]) {
      puts("fail");
      printf("****** cur_idx: %d, result[%d](%d) > result[%d](%d)\n", cur_idx, i, result[i], i+1, result[i+1]);
      res = 0;
    }
  //if (res)
  //  printf("cur_idx: %d, success\n", cur_idx);
}

char buf[MAX_SIZE << 3];
void store_result(size_t t_cnt) {
  char filename[16];
  sprintf(filename, "output_%u.txt", t_cnt);
  FILE *f = fopen(filename, "w");
  int tail = 0, print_cnt;
  for (size_t i = 0; i < input_size; i++) {
    print_cnt = sprintf(buf + tail, (i < input_size-1 ? "%d " : "%d"), result[i]);
    if (print_cnt < 0) {
      perror("sprintf");
      exit(8);
    }
    tail += print_cnt;
  }
  fwrite(buf, tail, 1, f);
  fclose(f);
}


int main(void) {
  tpool_t *pp = malloc(sizeof(tpool_t));
  read_input();
  //build range index
  build_range_arr(1, 0, input_size-1);

  
  struct timeval start, end;
  for (size_t t_cnt = 1; t_cnt <= MAX_THREAD; t_cnt++) {
    //init one_child_finish semaphores
    for (size_t i = 0; i < (1 << HEIGHT); i++) {
      SEM_INIT(&(one_child_finish[i]), 1);
    }
    
    memcpy(result, input, input_size * sizeof(int));
    gettimeofday(&start, NULL);

    do_sort(pp, t_cnt);

    gettimeofday(&end, NULL);
    printf("worker thread #%d, elapsed %f ms\n", t_cnt, tv_diffms(&start, &end));
    check_ans(0, input_size-1, -1);
    store_result(t_cnt);
  }
  free(pp);
  pp = NULL;
  return 0;
}