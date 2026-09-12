import Error;


fn create -> JWT (
   ip,
   port,
   module_name,
   module_pid,
) {
    jwt_data: {
       ip, //192.0.0.1
       port, //8008
       module_name, //youtube-module-rs
       module_pid, //whatever the engine sets, expect string
       iat, //unixtime.now()
       eat, //unixtime.now() + (math.random()*10min)+1min
       process_location, // ie: connection, pre, in, or post
    }

}

fn verify(JWT) -> Box<String, error::err> /*perm level, ie: "", sponser, mod, admin, owner*/ {
    
}
