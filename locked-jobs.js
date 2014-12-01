
var _ = require('lodash')
var moment = require('moment-timezone');
var util = require('util');

exports.add_job = add_job;
exports.remove_job = remove_job;
exports.status = status;

var DEFAULT_INTERVAL_MS = 60*1000;
var JOB_DEFAULTS = {
    last_run_time: false,
    next_run_time: false,
};

var g_job_list = [];
var g_add_queue_interval = false;
var g_run_list = [];
var g_held_lock_list = [];
var g_running_job_list = [];

var g_last_job_id = 0;

function add_job(job)
{
    var new_job = _.extend({},JOB_DEFAULTS,job);
    if( new_job.interval_ms )
    {
        new_job.last_run_time = Date.now();
    }
    else if( new_job.run_at )
    {
        set_next_run_time(new_job);
    }
    else
    {
        throw "interval_ms or run_at are required.";
    }
    new_job.job_id = g_last_job_id++;
    g_job_list.push(new_job);

    start_add_queue();
}

function remove_job(tag)
{
}

function status()
{
    return {
        job_list: g_job_list,
        run_list: g_run_list,
        held_lock_list: g_held_lock_list,
        running_job_list: g_running_job_list,
    };
}

function start_add_queue()
{
    if( g_add_queue_interval !== false )
    {
        clearInterval(g_add_queue_interval);
        g_add_queue_interval = false;
    }
    if( g_job_list.length > 0 )
    {
        var interval_ms = _.reduce(g_job_list,
        function(memo,job)
        {
            if( job.interval_ms )
            {
                memo = Math.min(memo,job.interval_ms);
            }
            return memo;
        },DEFAULT_INTERVAL_MS);
        g_add_queue_interval = setInterval(add_queue,interval_ms);
    }
}

// Expand this to handle more interesting run_at config
function set_next_run_time(job)
{
    var time = moment.tz.apply(null,job.run_at).valueOf();
    if( time < Date.now() )
    {
        time += 24*60*60*1000;
    }
    job.next_run_time = time;
}

function add_queue()
{
    _.each(g_job_list,function(job)
    {
        if( !_.findWhere(g_run_list,{ job_id: job.job_id }) )
        {
            if( job.interval_ms )
            {
                if( Date.now() - job.last_run_time > job.interval_ms )
                {
                    g_run_list.push(job);
                }
            }
            else if( job.next_run_time )
            {
                if( job.next_run_time < Date.now() )
                {
                    g_run_list.push(job);
                    set_next_run_time(job);
                }
            }
        }
    });
    run_jobs();
}

function run_jobs()
{
    var all_lock_list = [];
    _.each(g_job_list,function(job)
    {
        _.each(job.lock_list,function(job_lock)
        {
            if( typeof job_lock == 'string' )
            {
                all_lock_list.push(job_lock);
            }
        });
    });
    all_lock_list = _.uniq(all_lock_list);

    g_run_list = _.filter(g_run_list,function(job)
    {
        var is_runnable = _.all(job.lock_list,function(job_lock)
        {
            var ret = true;
            if( typeof job_lock == 'string' )
            {
                if( g_held_lock_list.indexOf(job_lock) > -1 )
                {
                    ret = false;
                }
            }
            else if( util.isRegExp(job_lock) )
            {
                var has_match = _.any(g_held_lock_list,function(held_lock)
                {
                    return job_lock.test(held_lock);
                });
                ret = !has_match;
            }
            return ret;
        });
        if( is_runnable )
        {
            var aquire_lock_list = [];
            _.each(job.lock_list,function(job_lock)
            {
                if( typeof job_lock == 'string' )
                {
                    aquire_lock_list.push(job_lock);
                }
                else if( util.isRegExp(job_lock) )
                {
                    _.each(all_lock_list,function(lock)
                    {
                        if( job_lock.test(lock) )
                        {
                            aquire_lock_list.push(lock);
                        }
                    });
                }
            });
            Array.prototype.push.apply(g_held_lock_list,aquire_lock_list);
            setImmediate(function()
            {
                job.last_run_time = Date.now();
                g_running_job_list.push(job);
                job.func(function()
                {
                    g_held_lock_list = _.filter(g_held_lock_list,function(lock)
                    {
                        return aquire_lock_list.indexOf(lock) == -1;
                    });
                    _.remove(g_running_job_list,{ job_id: job.job_id });
                    setImmediate(run_jobs);
                });
            });
        }

        return !is_runnable;
    });
}
