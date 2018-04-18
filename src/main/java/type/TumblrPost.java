package type;

import java.util.Arrays;
import java.util.List;

public class TumblrPost {
    public Long id;
    public String type;
    public String text;
    public String date;
    public String blog_name;
    public List<String> tags;

    public TumblrPost(Long id, String type, String text, String date, String blog_name, List<String> tags) {
        this.id = id;
        this.type = type;
        this.text = text;
        this.date = date;
        this.blog_name = blog_name;
        this.tags = tags;
    }

    public TumblrPost() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getBlog_name() {
        return blog_name;
    }

    public void setBlog_name(String blog_name) {
        this.blog_name = blog_name;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "type.TumblrPost{" +
                "id=" + id +
                ", text='" + text + '\'' +
                ", tags=" + tags.toArray().toString() +
                '}';
    }
}
