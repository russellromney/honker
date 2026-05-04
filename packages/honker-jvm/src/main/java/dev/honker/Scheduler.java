package dev.honker;

import java.time.Instant;
import java.util.Optional;

public final class Scheduler {
    private final Database db;

    Scheduler(Database db) {
        this.db = db;
    }

    public void add(String name, String queue, CronSchedule schedule, String payloadJson) {
        add(name, queue, schedule, payloadJson, ScheduleOptions.defaults());
    }

    public void add(String name, String queue, CronSchedule schedule, String payloadJson, ScheduleOptions options) {
        Long expires = options.expires() == null ? null : Durations.seconds(options.expires(), "schedule expires");
        db.transaction(tx -> tx.query(
            "SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?)",
            Params.of(name, queue, schedule.expression(), payloadJson, options.priority(), expires)
        ));
    }

    public boolean remove(String name) {
        return db.transaction(tx -> tx.query(
            "SELECT honker_scheduler_unregister(?) AS n",
            Params.of(name)
        ).get(0).getInt("n") > 0);
    }

    public int tick(Instant now) {
        String rows = db.transaction(tx -> tx.query(
            "SELECT honker_scheduler_tick(?) AS rows_json",
            Params.of(now.getEpochSecond())
        ).get(0).getString("rows_json"));
        return Json.objectArray(rows).size();
    }

    public Optional<Instant> soonest() {
        long t = db.transaction(tx -> tx.query("SELECT honker_scheduler_soonest() AS t").get(0).getLong("t"));
        return t == 0L ? Optional.empty() : Optional.of(Instant.ofEpochSecond(t));
    }

    public SchedulerHandle run() {
        return run(SchedulerOptions.defaults());
    }

    public SchedulerHandle run(SchedulerOptions options) {
        return new SchedulerHandle(db, this, options);
    }
}
