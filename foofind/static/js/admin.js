$(document).ready(function(){
    var c=function(x){if(x.search("/") == 0) return x.slice(1);return x;}, path=c(window.location.pathname),
    path=c((path.search("/")>0)?path.slice(path.search("/")):path);
    if(path=="admin/deploy"){
        if($(".deploybtn, .publishbtn").attr("disabled")=="disabled"){ // AJAX polling de estado de deploy
            var inter=setInterval(function(){
                $.getJSON("/en/admin/deploy/status", function(data, textStatus){
                    if(textStatus=="success"){
                        if(data["available"]){
                            clearInterval(inter);
                            $(".in_progress_message").remove();
                            $(".deploybtn, .publishbtn").removeAttr("disabled");
                            }
                        $("#stderr").text(data["stderr"]);
                        $("#stdout").text(data["stdout"]);
                        var dl = $("#backup_list"), dd, section;
                        dl.empty();
                        for(var i0=0,j0=data["backups"].length;i0<j0;i0++){
                            section=data["backups"][i0];
                            dd = "";
                            dl.append('<dt>'+section[0]+'</dt>');
                            for(var i1=0,j1=section[1].length;i1<j1;i1++){
                                dd = dd + "<a href=\"/en/admin/deploy/file/"+section[1][i1]+"\">"+section[1][i1]+"</a>";
                                if(i1<j1-1) dd = dd + " | ";
                                }
                            dl.append('<dd>'+dd+'</dd>');
                            }
                        var select=$("#publish_mode");
                        select.empty();
                        for(var i0=0,j0=data["publish"].length;i0<j0;i0++){
                            section=data["publish"][i0];
                            select.append('<option value="'+section[0]+'">'+section[1]+'</option>');
                            }
                        select.children().first().attr("selected","selected");
                        }
                    });
                }, 2000);
            }
        // Desplegable de botones de deploy
        var showbtn=$('<a href="javascript:" class="showmore">+</a>')
            .addClass("toggler")
            .click(function(){
                var cur=$(".deploy_advanced").css("display")=="none";
                $(".deploy_advanced").css("display",cur?"inherit":"none");
                showbtn.text(cur?"-":"+");
                });
        $(".deploy_advanced").css("display","none");
        $(".deploy_buttons")
            .append("<dd></dd>")
            .children(":last-child")
            .append(showbtn);
        }
    });
