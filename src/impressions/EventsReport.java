package impressions;

import java.util.HashSet;
import java.util.Set;

public class EventsReport {
	String advertiserId;
	int eventCount;
	Set<String> userId;

	public EventsReport(String advId, int count, String uid) {
		this.advertiserId = advId;
		this.eventCount = count;
		this.userId = new HashSet<String>();
		userId.add(uid);
	}
	public EventsReport(String advId) {
		this.advertiserId = advId;
		this.eventCount = 0;
		this.userId = new HashSet<String>();
	}
	public String getAdvertiserId() {
		return advertiserId;
	}

	public void setAdvertiserId(String advertiserId) {
		this.advertiserId = advertiserId;
	}

	public int getEventCount() {
		return eventCount;
	}

	public void setEventCount(int eventCount) {
		this.eventCount = eventCount;
	}

	public Set<String> getUserId() {
		return userId;
	}

	public void setUserId(String uId) {
		this.userId.add(uId);
	}
}
