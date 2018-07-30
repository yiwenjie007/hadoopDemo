
public enum RedisCacheKeyEnum {

    /**
     * actid:活动id,uid:用户id,ps:pagesize,pn:pageNo,instid:instanceid
     */
    ;
    /**
     * 前缀
     */
    private String prefix;
    /* 缓存key */
    private String key;

    /* 缓存时间 */
    private int second;

    /* 缓存中文说明 */
    private String comment;

    /**
     * 构造函数
     *
     * @param key
     * @param second
     * @param comment
     */
    private RedisCacheKeyEnum(String prefix, String key, int second, String comment) {
        this.prefix=prefix;
        this.key = key;
        this.second = second;
        this.comment = comment;
    }

    public String getPrefix() {
        return prefix;
    }

    /**
     * getter
     */

    public String getKey() {
        return key;
    }

    public int getSecond() {
        return second;
    }

    public String getComment() {
        return comment;
    }


    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(60);
        sb.append(this.getPrefix()).append(" \t\t ").append(this.getKey()).append(" \t\t ")
                .append(this.getSecond())
                .append(" \t ").append(this.getComment());
        return sb.toString();
    }

}
