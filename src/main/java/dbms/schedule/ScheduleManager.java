package dbms.schedule;

public class ScheduleManager {
    public static final ScheduleManager instance = new ScheduleManager();

    public ScheduleManager getInstance() {
        return instance;
    }
}
