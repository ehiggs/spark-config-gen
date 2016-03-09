function round_mb(mem) {
    ///Given memory amount in bytes, round to the best mb value based on
    ///Hortonworks' script:
    ///https://github.com/hortonworks/hdp-configuration-utils
    var mem_in_mb = mem / (Math.pow(1024, 2));
    if (mem_in_mb > 4096) {
        denom = 1024;
    } else if (mem_in_mb > 2048) {
        denom = 512;
    } else if (mem_in_mb > 1024) {
        denom = 256;
    } else {
        denom = 128;
    }

    var floor = Math.floor;
    return floor(floor(mem_in_mb/denom) * denom);
}

function parse_memory(memstr) {
    ///Given a string representation of memory, return the memory size in
    ///bytes. It supports up to terabytes.

    ///No size defaults to byte:
    var pat = /[bBkKmMgGtT]/;
    var size = memstr.match(pat);
    var coef, exp='';

    if (size !== undefined) {
        coef = parseFloat(memstr.slice(0, size.index));
        exp = memstr.slice(size.index, size.index + size.length);
    } else {
        coef = parseFloat(memstr);
        if (coef === inf) {
            return -1;
        }
    }

    if (['', 'b', 'B'].indexOf(exp) != -1){
        return coef;
    } else if (['k', 'K'].indexOf(exp) != -1) {
        return coef * 1024;
    } else if (['m', 'M'].indexOf(exp) != -1) {
        return coef * Math.pow(1024, 2);
    } else if (['g', 'G'].indexOf(exp) != -1) {
        return coef * Math.pow(1024, 3);
    } else if (['t', 'T'].indexOf(exp) != -1) {
        return coef * Math.pow(1024, 4);
    }
    return -1;
}

function config_generator_generator(dflt_cores_per_executor) {

    var config_generator = function (num_nodes, cores_per_node, memory_per_node, tempdir) {
        memory_per_node = parse_memory(memory_per_node);
        var cores_per_executor = Math.min(dflt_cores_per_executor, cores_per_node);
        var instances_per_node = cores_per_node / cores_per_executor;
        var instances = Math.max((num_nodes * instances_per_node) -1, 1);
        var memory = round_mb(memory_per_node / instances_per_node);
        var spark_defaults = {
            'spark.executor.cores': cores_per_executor,
            'spark.executor.instances': instances,
            'spark.executor.memory':  memory + 'M',
            'spark.local.dir': tempdir,
        };
        var spark_submit = {
            'executor-memory': memory + 'M',
            'num-executors': instances,
        };
        return {'spark_defaults': spark_defaults, 'spark_submit': spark_submit};
    };
    return config_generator;
}

/// spark-defaults.conf uses spaces to separate keys and values.
function fmt_spark_default(config) {
    var ret = "";
    for (var key in config) {
        ret += key + " " + config[key] + '<br/>';
    }
    console.log(ret);
    return ret;
}

function fmt_spark_submit(config) {
    var ret = "spark-submit --master yarn --deploy-mode cluster";
    for (var key in config) {
        ret += "--" + key + " " + config[key] + " ";
    }
    console.log(ret);
    return ret;
}

/// Hanythingondemand hod.conf uses '=' to separate keys and values.
function fmt_hod_conf(config) {
    var ret = "[spark-default.conf]<br/>";
    for (var key in config) {
        ret += key + "=" + config[key] + '<br/>';
    }
    console.log(ret);
    return ret;
}

function SparkConfigCalculator() {
    this.cluster_types = [
    {'name': "General use case", "config_generator": config_generator_generator(2)},
    {'name': "High Memory / Low Parallelism (e.g. Random Forest)", "config_generator": config_generator_generator(4)},
    ];
    this.chosen_cluster_type = ko.observable(this.cluster_types[0]);
    this.num_nodes = '16';
    this.num_cores = '20';
    this.memory_per_node = '64G';
    this.tempdir = '/tmp';
    this.spark_default = ko.observable();
    this.spark_submit= ko.observable();
    this.hod_conf = ko.observable();

    this.generate = function() {
        var configs = this.chosen_cluster_type().config_generator(this.num_nodes, this.num_cores, this.memory_per_node, this.tempdir);
        this.spark_default(fmt_spark_default(configs.spark_defaults));
        this.spark_submit(fmt_spark_submit(configs.spark_submit));
        this.hod_conf(fmt_hod_conf(configs.spark_defaults));
    };

    this.generate();
}

ko.applyBindings(new SparkConfigCalculator());

