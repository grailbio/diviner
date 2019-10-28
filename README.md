# Diviner

Diviner is a serverless machine learning and hyperparameter tuning platform.
Diviner runs *studies* on behalf of a user;
each study comprises a set of hyperparameters
(e.g., learning rate, data augmentation policy, loss functions)
and instructions for how to instantiate a *trial* based on a set of concrete
hyperparameter values.
Diviner then manages trial execution,
book-keeping,
and hyperparameter optimization based on past trials.

Diviner can be used as a scriptable tool or in Go programs
through a [Go package](https://godoc.org/github.com/grailbio/diviner).

- build status: [![Build Status](https://travis-ci.org/grailbio/diviner.svg?branch=master)](https://travis-ci.org/grailbio/diviner)
- issues: [github.com/grailbio/diviner/issues](https://github.com/grailbio/diviner/issues)
- API docs: [godoc.org/github.com/grailbio/diviner](https://godoc.org/github.com/grailbio/diviner)
- install diviner: `GO111MODULE=on go get -u github.com/grailbio/diviner/cmd/diviner`

# Studies, trials, and runs

Diviner defines a data model that is rooted in user-defined *studies*.
A *study* contains all the information needed to conduct a number of *trials*,
including a set of hyperparameters over which to conduct the study.
A *trial* is an instantiation of a set of valid hyperparameter values.
A *run* is a trial attempt; runs may fail or be retried.

Diviner stores studies and runs in an underlying database, keyed by study names.
The database is used to construct leaderboards that show the best-performing
hyperparameter combinations for a study.
The database can also be used to query pending runs
and detailed information about each.

The diviner tool interprets study definitions written in
[Starlark](https://docs.bazel.build/versions/master/skylark/language.html).
The study definitions include the hyperparameters definitions
and a function that determines how to conduct a run based on a set of parameter values,
selected by an optimizer (called an oracle).

# Example: optimizing MNIST with PyTorch

In this example, we'll run a hyperparameter search on the
[example PyTorch](https://github.com/pytorch/examples/blob/master/mnist/main.py)
convolutional neural network on the MNIST dataset.

By default, Diviner uses the dynamoDB table called "diviner".
To set this up, we have to run `diviner create-table` before we
begin to use Diviner.

```
$ diviner create-table
```

Now we're ready to define our study and instruct Diviner how to run trials.

First, create a file called `mnist.dv`.
We are interested in running training on GPU instances in AWS EC2,
so the first thing we do is to define an ec2 system for the trainers to run:

```
ec2gpu = ec2system(
    "ec2gpu",
    region="us-west-2",
    ami="ami-01a4e5be5f289dd12",
    security_group="SECURITY_GROUP",
    instance_type="p3.2xlarge",
    disk_space=100,
    data_space=500,
    on_demand=True,
    flavor="ubuntu")
```

The AMI named here is the
[AWS DLAMI](https://docs.aws.amazon.com/dlami/latest/devguide/options.html)
in us-west-2 as of this writing.
The security group should give external access to port 443 (HTTPS).

We'll run our examples on p3.2xlarge instances,
which are the smallest GPU instances provided by EC2.

Next, we'll define a function that is called with a set of parameter values.
The function, run_mnist, returns a run configuration based on the provided
parameter values. The run config defines how a trial is to be run given these parameters.
In our case, we keep it very simple: we use the pytorch-provided docker image to invoke
the mnist example with the provided parameter values.

```
def run_mnist(params):
    return run_config(
        system=ec2gpu,
        script="""
nvidia-docker run -i pytorch/pytorch bash -x <<EOF
git clone https://github.com/pytorch/examples.git
python examples/mnist/main.py \
	--batch-size %(batch_size)d \
	--lr %(learning_rate)f \
	--epochs %(epochs)d  2>&1 | \
	awk '/^Test set:/ {gsub(",", "", \$5); print "METRICS: loss="\$5} {print}'
EOF
""" % params)
```

The only thing of note here is that we're using awk to pull out the test losses reported
by the PyTorch trainer and formatting them in the manner expected by Diviner.
(Any line in the process stdout beginning with "METRICS: " and followed by
a set of key=value pairs are interpeted by Diviner as metrics reported by the trial.)
The run config also defines the system definition on which to run the trial.

Now that we have a system definition and a run function,
we can define our study. The study declares the set of parameters
and their ranges. In this case, we're using discrete parameter ranges,
but users can also define continuous ranges. Starlark intrinsics available
in Diviner are documented [here](https://godoc.org/github.com/grailbio/diviner/script).
We define the study's objective to minimize the metric "loss"
(as reported by the runner, above). The oracle, grid_search, defines how
the parameter space is to be explored. Grid searching exhaustively explores
the parameter space.

```
study(
    name="mnist",
    params={
        "epochs": discrete(1, 10, 50, 100),
        "batch_size": discrete(32, 64, 128, 256),
        "learning_rate": discrete(0.001, 0.01, 0.1, 1.0),
    },
    objective=minimize("loss"),
    oracle=grid_search,
    run=run_mnist,
)
```

Finally, we can now run the study. We run the study in "streaming" mode,
meaning that new trials are started as soon as capacity allows. The `-trials`
argument determines how many trials may be run in parallel. (And in our case,
how many EC2 GPU instances are created at a time.)

```
$ diviner run -stream -trials 5 mnist.dv
```

While the study is running, we can query the database. For example,
to see the current set of trials running:

```
$ diviner list -runs -state pending -s
mnist:9  6:02PM 11m59s pending running: Train Epoch: 16 [45120/60000 (75%)] Loss: 0.171844
mnist:10 6:08PM 5m29s  pending running: Train Epoch: 9 [14080/60000 (23%)]  Loss: 0.178012
mnist:11 6:09PM 5m14s  pending running:
mnist:12 6:09PM 5m14s  pending running: Train Epoch: 5 [56320/60000 (94%)] Loss: 0.380566
mnist:13 6:09PM 4m59s  pending running: Train Epoch: 7 [19200/60000 (32%)] Loss: 0.069407
```

(The last column(s) in this output shows the last line printed to standard output.)
We can also examine the details of particular run. Runs are named by the study
and an index.

```
$ diviner info mnist:9
run mnist:9:
    state:     pending
    created:   2019-10-25 18:02:14 -0700 PDT
    runtime:   13m29.972611766s
    restarts:  0
    replicate: 0
    values:
        batch_size:    32
        epochs:        50
        learning_rate: 0.001
    metrics:
        loss: 0.0398
```

Here we can see the parameter values used in this run,
the latest reported metrics, and some other relevant metadata.
Passing `-v` to `diviner info` gives even more detail, including
all of the reported metrics and the rendered script.

```
$ diviner info -v mnist:9
run mnist:9:
    state:     pending
    created:   2019-10-25 18:02:14 -0700 PDT
    runtime:   13m59.973265371s
    restarts:  0
    replicate: 0
    values:
        batch_size:    32
        epochs:        50
        learning_rate: 0.001
    metrics[0]:
        loss: 0.2996
    metrics[1]:
        loss: 0.1848
    ...
    metrics[17]:
        loss: 0.0398
    script:
        nvidia-docker run -i pytorch/pytorch bash -x <<EOF
        git clone https://github.com/pytorch/examples.git
        python examples/mnist/main.py  --batch-size 32  --lr 0.001000  --epochs 50  2>&1 |  awk '/^Test set:/ {gsub(",", "", \$5); print "METRICS: loss="\$5} {print}'
        EOF
```

After Diviner has accumulated a number of trials,
we can request the current leaderboard:

```
$ diviner leaderboard mnist
study    replicates loss   batch_size epochs learning_rate
mnist:21 0          0.0255 32         10     0.01
mnist:28 0          0.0265 256        50     0.01
mnist:9  0          0.027  32         50     0.001
mnist:14 0          0.0285 64         100    0.001
mnist:27 0          0.029  128        50     0.01
mnist:13 0          0.0298 32         100    0.001
...
```

This tells us the best hyperparameters (so far) for this MNIST classification
task is batch_size=32, epochs=10, and learning_rate=0.01.

In addition to tracking studies and runs, Diviner maintains logs for each run.
This can be useful when debugging issues or monitoring ongoing jobs.
For example to view the logs of the best entry in the above study:

```
$ diviner logs mnist:21
diviner: started run (try 1) at 2019-10-25 18:41:57.84792 -0700 PDT on https://ec2-52-88-125-113.us-west-2.compute.amazonaws.com/
+ git clone https://github.com/pytorch/examples.git
Cloning into 'examples'...
+ python examples/mnist/main.py --batch-size 32 --lr 0.010000 --epochs 10
+ awk '/^Test set:/ {gsub(",", "", $5); print "METRICS: loss="$5} {print}'
9920512it [00:01, 8438927.61it/s]
32768it [00:00, 138802.83it/s]
1654784it [00:00, 2387828.32it/s]
8192it [00:00, 53052.53it/s]            Downloading http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz to ../data/MNIST/raw/train-images-idx3-ubyte.gz
Extracting ../data/MNIST/raw/train-images-idx3-ubyte.gz to ../data/MNIST/raw
Downloading http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz to ../data/MNIST/raw/train-labels-idx1-ubyte.gz
Extracting ../data/MNIST/raw/train-labels-idx1-ubyte.gz to ../data/MNIST/raw
Downloading http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz to ../data/MNIST/raw/t10k-images-idx3-ubyte.gz
Extracting ../data/MNIST/raw/t10k-images-idx3-ubyte.gz to ../data/MNIST/raw
Downloading http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz to ../data/MNIST/raw/t10k-labels-idx1-ubyte.gz
Extracting ../data/MNIST/raw/t10k-labels-idx1-ubyte.gz to ../data/MNIST/raw
Processing...
Done!
Train Epoch: 1 [0/60000 (0%)]	Loss: 2.301286
Train Epoch: 1 [320/60000 (1%)]	Loss: 2.246368
Train Epoch: 1 [640/60000 (1%)]	Loss: 2.143235
Train Epoch: 1 [960/60000 (2%)]	Loss: 2.020995
Train Epoch: 1 [1280/60000 (2%)]	Loss: 1.889598
Train Epoch: 1 [1600/60000 (3%)]	Loss: 1.387788
Train Epoch: 1 [1920/60000 (3%)]	Loss: 1.103807
...
```

